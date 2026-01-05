import scrapy
import json
import signal
import os
from collections import deque
from scrapy.http import Request
from typing import Dict, List, Optional, Set
import asyncio
from scrapy import signals

class SpotifyToken:
    """Container for Spotify authentication tokens and headers"""
    def __init__(self, headers: Dict[str, str]):
        self.headers = headers
        self.authorization = headers.get("authorization", "")
        self.client_token = headers.get("client-token", "")
        self.failed_count = 0
    
    def to_headers(self) -> Dict[str, str]:
        # Return all captured headers
        return self.headers.copy()


class SpotifyGraphSpider(scrapy.Spider):
    name = "spotify_graph"
    
    custom_settings = {
        # Enable Playwright for token generation
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
            ],
        },
        
        # Force new context for each request to get different tokens
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
            }
        },
        
        "PLAYWRIGHT_MAX_CONTEXTS": 10,
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN":1, 
        "DOWNLOAD_DELAY": 0.4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1,
        "AUTOTHROTTLE_DEBUG": False,
        
        "RETRY_ENABLED": True, 
        "RETRY_HTTP_CODES": [500, 502, 503, 504], ## I removed 429 so i can retry manually
        "RETRY_TIMES": 2,
        
        "TWISTED_REACTOR_CLOSE_TIMEOUT": 5,
        
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, start_user, depth='2', max_followers='100', checkpoint_file=None, resume_data=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_user = start_user
        self.max_depth = int(depth)
        self.max_followers = int(max_followers)
        self.checkpoint_file = checkpoint_file or f'checkpoint_{start_user}.json'
        self.should_save_checkpoint = False
        self.resume_data = json.loads(resume_data) if resume_data else None
        
        # Token pool management
        self.tokens: deque[SpotifyToken] = deque()
        self.min_tokens = 10
        self.max_tokens = 15
        self.tokens_being_generated = 0
        self.token_request_counter = 0  # Counter to ensure unique contexts
        
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ]
        
        # Tracking
        self.visited_users = set()
        self.pending_requests = deque()
        self.users_scraped = 0
        
        # Rate limiting
        self.rate_limited_count = 0
        self.consecutive_rate_limits = 0
        self.backoff_delay = 1.0  # Start with 1 second
        
        # Queue of users to scrape (for checkpoint/resume)
        self.user_queue: deque = deque()  # (user_id, depth, known_name, known_followers_count)
        
        # Register signal handler
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Restore from checkpoint if resume_data provided
        if self.resume_data:
            self.restore_from_checkpoint(self.resume_data)
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals (Ctrl+C)"""
        self.logger.warning(f"Received signal {signum}, saving checkpoint before exit...")
        self.should_save_checkpoint = True
        self.save_checkpoint()
        # Re-raise to let Scrapy handle the shutdown
        raise KeyboardInterrupt()
    
    def save_checkpoint(self):
        """Save current state to checkpoint file for later resumption"""
        # Collect pending user requests from the queue
        queue_items = list(self.user_queue)
        
        # Also extract user info from pending_requests
        for req in self.pending_requests:
            user_id = req.meta.get('user_id')
            depth = req.meta.get('depth')
            known_name = req.meta.get('known_name')
            known_followers_count = req.meta.get('known_followers_count')
            if user_id and depth is not None:
                queue_items.append((user_id, depth, known_name, known_followers_count))
        
        checkpoint_data = {
            'start_user': self.start_user,
            'max_depth': self.max_depth,
            'max_followers': self.max_followers,
            'visited_users': list(self.visited_users),
            'user_queue': queue_items,
            'users_scraped': self.users_scraped,
            'rate_limited_count': self.rate_limited_count,
        }
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        self.logger.info(f"Checkpoint saved to {self.checkpoint_file}")
        self.logger.info(f"  - Visited users: {len(self.visited_users)}")
        self.logger.info(f"  - Queue size: {len(queue_items)}")
        self.logger.info(f"  - Users scraped: {self.users_scraped}")
    
    @classmethod
    def load_checkpoint(cls, checkpoint_file: str) -> Optional[dict]:
        """Load checkpoint data from file"""
        if not os.path.exists(checkpoint_file):
            return None
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    
    def restore_from_checkpoint(self, checkpoint_data: dict):
        """Restore spider state from checkpoint data"""
        self.visited_users = set(checkpoint_data.get('visited_users', []))
        self.users_scraped = checkpoint_data.get('users_scraped', 0)
        self.rate_limited_count = checkpoint_data.get('rate_limited_count', 0)
        
        # Restore user queue
        queue_items = checkpoint_data.get('user_queue', [])
        for item in queue_items:
            if len(item) >= 2:
                user_id, depth = item[0], item[1]
                known_name = item[2] if len(item) > 2 else None
                known_followers_count = item[3] if len(item) > 3 else None
                self.user_queue.append((user_id, depth, known_name, known_followers_count))
                # Remove from visited_users so they can be re-requested
                # (they were marked visited when request was created, but not yet processed)
                self.visited_users.discard(user_id)
        
        self.logger.info(f"Restored from checkpoint:")
        self.logger.info(f"  - Visited users: {len(self.visited_users)}")
        self.logger.info(f"  - Queue size: {len(self.user_queue)}")
        self.logger.info(f"  - Users scraped: {self.users_scraped}")
    
    def closed(self, reason):
        """Called when spider closes"""
        self.logger.info(f"Spider closing. Reason: {reason}")
        self.logger.info(f"Total users scraped: {self.users_scraped}")
        self.logger.info(f"Total rate limits encountered: {self.rate_limited_count}")
        self.logger.info(f"Tokens in pool at close: {len(self.tokens)}")
        self.logger.info(f"Pending requests at close: {len(self.pending_requests)}")
        self.logger.info(f"User queue at close: {len(self.user_queue)}")
        
        # Save checkpoint on any close if there's remaining work
        if len(self.user_queue) > 0 or len(self.pending_requests) > 0:
            self.save_checkpoint()

    def start_requests(self):
        """Entry point: start token generation and queue first user"""
        self.logger.info(f"Starting scrape from {self.start_user} with max depth {self.max_depth}")
        
        # Generate initial token pool
        for _ in range(self.min_tokens):
            yield self.create_token_request()
        
        # If resuming from checkpoint, process the queue
        if self.user_queue:
            self.logger.info(f"Resuming from checkpoint with {len(self.user_queue)} users in queue")
            for user_id, depth, known_name, known_followers_count in self.user_queue:
                req = self.create_follower_request(user_id, depth, known_name=known_name, known_followers_count=known_followers_count)
                if req:
                    yield req
            self.user_queue.clear()  # Clear after generating requests
        else:
            # Initial user request with depth 0
            yield self.create_follower_request(self.start_user, 0)

    def create_token_request(self):
        self.tokens_being_generated += 1
        self.token_request_counter += 1

        context_name = f"token_context_{self.token_request_counter}"
        import random
        user_agent = random.choice(self.user_agents)
        
        return Request(
            url="https://open.spotify.com/",
            callback=self.parse_token_page,
            errback=self.errback_token,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_init_callback": self.init_token_capture,
                "playwright_context": context_name,  # Unique context per request
                "playwright_context_kwargs": {
                    "ignore_https_errors": True,
                    "user_agent": user_agent,
                },
            },
            dont_filter=True,
            priority=1000  # Highest priority
        )

    async def init_token_capture(self, page, request):
        captured_tokens = []
        
        async def handle_request(route, pw_request):
            headers = pw_request.headers
            
            url = pw_request.url
            if "spclient.wg.spotify.com" in url  or "api.spotify.com" in url or "api-partner.spotify.com" in url:
                if "authorization" in headers and "client-token" in headers:
                    token_headers = {}
                    
                    for key, value in headers.items():
                        # Skip some headers that are request-specific
                        if key.lower() not in ['content-length', 'host', 'connection']:
                            token_headers[key] = value
                    
                    if token_headers.get("authorization") and token_headers.get("client-token"):
                        # Avoid duplicates in this capture session
                        is_duplicate = False
                        for existing in captured_tokens:
                            if existing.get("authorization") == token_headers.get("authorization"):
                                is_duplicate = True
                                break
                        
                        if not is_duplicate:
                            captured_tokens.append(token_headers)
                            auth_preview = token_headers.get("authorization", "")[:50]
                            self.logger.info(f"Captured headers from {url[:80]}... Auth: {auth_preview}...")
            
            await route.continue_()
        
        await page.route("**/*", handle_request)
        
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
            await page.wait_for_timeout(3000)
        except Exception as e:
            self.logger.warning(f"Timeout waiting for page load: {e}")

        request.meta["captured_tokens"] = captured_tokens

    def parse_token_page(self, response):
        page = response.meta.get("playwright_page")
        captured_tokens = response.meta.get("captured_tokens", [])

        for token_headers in captured_tokens:
            token = SpotifyToken(token_headers)
            
            # Check if token already exists (compare authorization header)
            existing = False
            for existing_token in self.tokens:
                if existing_token.authorization == token.authorization:
                    existing = True
                    break
            
            if not existing:
                self.tokens.append(token)
                self.logger.info(f"Added new token to pool. Total tokens: {len(self.tokens)}")
        

        if page:

            try:
                asyncio.ensure_future(page.context.close())
            except Exception as e:
                self.logger.debug(f"Error closing page context: {e}")
        
        self.tokens_being_generated -= 1
        
        if captured_tokens:
            self.logger.info(f"Successfully captured {len(captured_tokens)} token(s) with full headers from this session")
        else:
            self.logger.warning("No tokens captured from this session. Page may not have made API calls yet.")
        
        # Process pending requests if we have tokens
        yield from self.process_pending_requests()

    def errback_token(self, failure):
        """Handle token generation failures"""
        self.tokens_being_generated -= 1
        self.logger.error(f"Token generation failed: {failure}")
        
        # Only retry if we don't have enough tokens yet
        if len(self.tokens) < self.min_tokens and self.tokens_being_generated == 0:
            yield self.create_token_request()

    def create_follower_request(self, user_id: str, depth: int, is_retry: bool = False, known_name: str = None, known_followers_count: int = None):
        """Create an API request to fetch user followers
        
        Args:
            user_id: Spotify user ID
            depth: Current distance from start user (0 = start user)
            is_retry: Whether this is a retry of a failed request
            known_name: Display name from parent's profile data
            known_followers_count: Follower count from parent's profile data
        """
        if not is_retry and user_id in self.visited_users:
            return None
        
        if depth > self.max_depth:
            self.logger.debug(f"Rejecting {user_id} - depth {depth} > max_depth {self.max_depth}")
            return None
        
        if not is_retry:
            self.visited_users.add(user_id)
        
        self.logger.debug(f"Creating request for {user_id} at depth {depth} (retry={is_retry})")
        
        url = f"https://spclient.wg.spotify.com/user-profile-view/v3/profile/{user_id}/followers?market=from_token"
        
        request = Request(
            url=url,
            callback=self.parse_followers,
            errback=self.errback_followers,
            meta={
                "user_id": user_id,
                "depth": depth,
                "known_name": known_name,
                "known_followers_count": known_followers_count,
                "playwright": False, 
                "handle_httpstatus_list": [400, 401, 403, 404, 429, 500, 502, 503]
            },
            priority=(self.max_depth - depth) * 1000,  # Higher remaining depth = higher priority for BFS
            dont_filter=True
        )
        
        # Try to attach a token
        if self.tokens:
            token = self.tokens[0]
            self.tokens.rotate(-1)  # Rotate for next request
            request.headers.update(token.to_headers())
            request.meta['token_auth'] = token.authorization  # Track which token was used
            # Assign a download slot based on the token's hash to spread requests
            # This helps Scrapy manage concurrency per token more effectively.
            request.meta['download_slot'] = hash(token.authorization)
            return request
        else:
            self.logger.warning(f"No tokens available, queuing request for {user_id}")
            self.pending_requests.append(request)
            
            if self.tokens_being_generated == 0:
                return self.create_token_request()
            return None

    async def parse_followers(self, response):
        """Parse the followers API response"""
        user_id = response.meta["user_id"]
        depth = response.meta["depth"]
        known_name = response.meta.get("known_name")
        known_followers_count = response.meta.get("known_followers_count")
        results = []
        
        if response.status == 401:
            self.logger.warning(f"Token expired for {user_id} at depth {depth}, removing token")
            # Remove the specific token that was used for this request
            token_auth = response.meta.get('token_auth')
            if token_auth:
                initial_len = len(self.tokens)
                self.tokens = deque([t for t in self.tokens if t.authorization != token_auth])
                if len(self.tokens) < initial_len:
                    self.logger.info(f"Removed expired token. Pool size: {len(self.tokens)}")
            
            # Generate new token
            results.append(self.create_token_request())
            
            # Retry the request with same depth (mark as retry)
            results.append(self.create_follower_request(user_id, depth, is_retry=True, known_name=known_name, known_followers_count=known_followers_count))
            return results
        
        if response.status == 403:
            self.logger.error(f"Access forbidden for {user_id}")
            results.append({"id": user_id, "error": "forbidden", "depth": depth})
            return results
        
        if response.status == 429:
            self.rate_limited_count += 1
            self.logger.warning(
                f"Rate limited for {user_id} at depth {depth} (#{self.rate_limited_count}). "
                #f"Removing token and retrying."
            )
            
            auth_header = response.request.headers.get(b'authorization') or response.request.headers.get('authorization')
            
            if auth_header:
                auth_str = auth_header.decode('utf-8') if isinstance(auth_header, bytes) else auth_header
                initial_len = len(self.tokens)
                self.tokens = deque([t for t in self.tokens if t.authorization != auth_str])
                
                if len(self.tokens) < initial_len:
                    self.logger.info(f"Removed rate-limited token. Pool size: {len(self.tokens)}")
                    results.append(self.create_token_request())
            
            # Retry the request
            retry_req = self.create_follower_request(user_id, depth, is_retry=True, known_name=known_name, known_followers_count=known_followers_count)
            if retry_req:
                results.append(retry_req)
                
            return results
        
        # Parse successful response
        try:
            data = json.loads(response.text)
        except Exception as e:
            self.logger.error(f"Failed to decode JSON for {user_id}: {e}")
            results.append({"id": user_id, "error": str(e), "depth": depth})
            return results
        
        profiles = data.get("profiles", [])
        
        # Extract follower IDs and their profile data
        follower_ids = []
        follower_profiles = []  # (id, name, followers_count) tuples
        for profile in profiles:
            uri = profile.get("uri", "")
            if uri.startswith("spotify:user:"):
                fid = uri.split(":")[-1]
                follower_ids.append(fid)
                follower_profiles.append((fid, profile.get("name"), profile.get("followers_count")))
        
        found_follower_count = len(follower_ids)
        follower_count = known_followers_count if known_followers_count is not None else found_follower_count
        
        self.users_scraped += 1
        
        # Remove from user_queue now that we've successfully processed this user
        try:
            self.user_queue.remove((user_id, depth, known_name, known_followers_count))
        except ValueError:
            pass  # Not in queue (e.g., start user or retry)
        
        self.consecutive_rate_limits = 0
        
        results.append({
            "id": user_id,
            "name": known_name,
            "depth": depth,
            "followers_count": follower_count,
            "follower_profiles": follower_profiles,
        })
        
        self.logger.info(
            f"[{self.users_scraped}] Scraped {user_id} at depth {depth}: {follower_count} followers found, {known_followers_count} known. "
            f"(Rate limited: {self.rate_limited_count} times)"
        )
        
        # Continue BFS if not at max depth and not too many followers
        if depth < self.max_depth and follower_count <= self.max_followers:
            self.logger.debug(f"Creating {follower_count} child requests at depth {depth + 1} for {user_id}")
            for fid, name, fc in follower_profiles:
                # Add to user_queue for checkpoint tracking
                if fid not in self.visited_users:
                    self.user_queue.append((fid, depth + 1, name, fc))
                req = self.create_follower_request(fid, depth + 1, known_name=name, known_followers_count=fc)
                if req:
                    results.append(req)
        else:
            if depth >= self.max_depth:
                self.logger.debug(f"Stopping BFS for {user_id} - reached max depth (depth={depth}, max={self.max_depth})")
            elif follower_count > self.max_followers:
                self.logger.debug(f"Stopping BFS for {user_id} - too many followers ({follower_count} > {self.max_followers})")
        
        #if len(self.tokens) < self.min_tokens and self.tokens_being_generated == 0:
        if len(self.tokens) < 3 and self.tokens_being_generated == 0 and len(self.pending_requests) > 0:
            self.logger.info(f"Token pool low ({len(self.tokens)}/{self.min_tokens}), generating more")
            results.append(self.create_token_request())
        
        results.extend(self.process_pending_requests())
        return results

    def errback_followers(self, failure):
        """Handle follower request failures"""
        request = failure.request
        user_id = request.meta.get("user_id")
        depth = request.meta.get("depth")
        
        # Log more details to debug
        self.logger.error(f"Request failed for {user_id} (depth={depth}): {failure.value}")
        self.logger.debug(f"Failed request URL: {request.url}")
        
        yield {
            "id": user_id,
            "depth": depth,
            "error": str(failure.value)
        }

    def process_pending_requests(self):
        """Process queued requests when tokens become available"""
        while self.pending_requests and self.tokens:
            request = self.pending_requests.popleft()
            token = self.tokens[0]
            self.tokens.rotate(-1)
            request.headers.update(token.to_headers())
            yield request
