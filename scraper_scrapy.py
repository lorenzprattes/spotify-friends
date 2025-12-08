import scrapy
import json
from collections import deque
from scrapy.http import Request
from typing import Dict, List, Optional


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
        # Enable Playwright for token generation only
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
        
        # Properly close contexts to avoid pending task warnings
        "PLAYWRIGHT_MAX_CONTEXTS": 8,
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        
        # High concurrency for API requests (will be throttled dynamically)
        "CONCURRENT_REQUESTS": 16,  # Global concurrency limit
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10, 
        "DOWNLOAD_DELAY": 0.1,  # Start with a 1-second delay between requests to the same domain
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10, # Aim for an average of 1.5 concurrent requests
        "AUTOTHROTTLE_DEBUG": False, # Set to False for cleaner logs
        
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 8,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504], # Add 429 to retry codes
        
        # Close gracefully to avoid pending task warnings
        "TWISTED_REACTOR_CLOSE_TIMEOUT": 5,
        
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, start_user='jonas.f.rappold', depth='2', max_followers='100', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_user = start_user
        self.max_depth = int(depth)
        self.max_followers = int(max_followers)
        
        # Token pool management
        self.tokens: deque[SpotifyToken] = deque()
        self.min_tokens = 5
        self.max_tokens = 15
        self.tokens_being_generated = 0
        self.token_request_counter = 0  # Counter to ensure unique contexts
        
        # User agent pool for diversity
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
        ]
        
        # Tracking
        self.visited_users = set()
        self.pending_requests = deque()
        self.users_scraped = 0
        
        # Rate limiting
        self.rate_limited_count = 0
        self.consecutive_rate_limits = 0
        self.backoff_delay = 1.0  # Start with 1 second
    
    def closed(self, reason):
        """Called when spider closes"""
        self.logger.info(f"Spider closing. Reason: {reason}")
        self.logger.info(f"Total users scraped: {self.users_scraped}")
        self.logger.info(f"Total rate limits encountered: {self.rate_limited_count}")
        self.logger.info(f"Tokens in pool at close: {len(self.tokens)}")
        self.logger.info(f"Pending requests at close: {len(self.pending_requests)}")

    def start_requests(self):
        """Entry point: start token generation and queue first user"""
        self.logger.info(f"Starting scrape from {self.start_user} with max depth {self.max_depth}")
        
        # Generate initial token pool
        for _ in range(self.min_tokens):
            yield self.create_token_request()
        
        # Queue the initial user request at depth 0
        yield self.create_follower_request(self.start_user, 0)

    def create_token_request(self):
        """Create a Playwright request to harvest tokens from Spotify"""
        self.tokens_being_generated += 1
        self.token_request_counter += 1
        
        # Use a unique context name to force a fresh browser context
        # This ensures each token generation gets a completely isolated session
        context_name = f"token_context_{self.token_request_counter}"
        
        # Rotate through user agents for diversity
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
                    "user_agent": user_agent,  # Set unique user agent per context
                },
            },
            dont_filter=True,
            priority=1000  # Highest priority
        )

    async def init_token_capture(self, page, request):
        """Initialize Playwright page with network request interception to capture tokens"""
        captured_tokens = []
        
        async def handle_request(route, pw_request):
            """Intercept network requests and capture authentication headers"""
            headers = pw_request.headers
            
            # Look for Spotify API requests with auth headers
            url = pw_request.url
            if "spclient.wg.spotify.com" in url or "api.spotify.com" in url or "api-partner.spotify.com" in url:
                if "authorization" in headers and "client-token" in headers:
                    # Capture ALL headers from the request
                    token_headers = {}
                    
                    # Copy all relevant headers
                    for key, value in headers.items():
                        # Skip some headers that are request-specific
                        if key.lower() not in ['content-length', 'host', 'connection']:
                            token_headers[key] = value
                    
                    # Ensure we have the critical ones
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
            
            # Continue the request
            await route.continue_()
        
        # Set up request interception
        await page.route("**/*", handle_request)
        
        # Wait a bit for the page to make API calls
        # This gives Spotify time to load and make authenticated requests
        try:
            # Wait for navigation to complete
            await page.wait_for_load_state("networkidle", timeout=10000)
            # Give it a bit more time for lazy-loaded API calls
            await page.wait_for_timeout(3000)
        except Exception as e:
            self.logger.warning(f"Timeout waiting for page load: {e}")
        
        # Store captured tokens in request meta for later retrieval
        request.meta["captured_tokens"] = captured_tokens

    def parse_token_page(self, response):
        """Extract tokens from Playwright page network traffic"""
        page = response.meta.get("playwright_page")
        captured_tokens = response.meta.get("captured_tokens", [])
        
        # Add captured tokens to our pool
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
                self.logger.info(f"✓ Added new token to pool. Total tokens: {len(self.tokens)}")
        
        # Close the page
        if page:
            # Page will be closed automatically by scrapy-playwright
            pass
        
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

    def create_follower_request(self, user_id: str, depth: int, is_retry: bool = False):
        """Create an API request to fetch user followers
        
        Args:
            user_id: Spotify user ID
            depth: Current distance from start user (0 = start user)
            is_retry: Whether this is a retry of a failed request
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
                "playwright": False,  # Direct HTTP request
                "handle_httpstatus_list": [401, 403, 429]
            },
            priority=(self.max_depth - depth) * 1000,  # Higher remaining depth = higher priority for BFS
            dont_filter=True
        )
        
        # Try to attach a token
        if self.tokens:
            token = self.tokens[0]
            self.tokens.rotate(-1)  # Rotate for next request
            request.headers.update(token.to_headers())
            # Assign a download slot based on the token's hash to spread requests
            # This helps Scrapy manage concurrency per token more effectively.
            request.meta['download_slot'] = hash(token.authorization)
            return request
        else:
            # No tokens available, queue the request
            self.logger.warning(f"No tokens available, queuing request for {user_id}")
            self.pending_requests.append(request)
            
            # Trigger token generation if not already happening
            if self.tokens_being_generated == 0:
                return self.create_token_request()
            return None

    async def parse_followers(self, response):
        """Parse the followers API response"""
        user_id = response.meta["user_id"]
        depth = response.meta["depth"]
        results = []
        
        # Handle authentication errors
        if response.status == 401:
            self.logger.warning(f"Token expired for {user_id} at depth {depth}, removing token")
            # Remove the bad token (it's now at the end after rotation)
            if self.tokens:
                bad_token = self.tokens.pop()
                bad_token.failed_count += 1
            
            # Generate new token
            results.append(self.create_token_request())
            
            # Retry the request with same depth (mark as retry)
            results.append(self.create_follower_request(user_id, depth, is_retry=True))
            return results
        
        if response.status == 403:
            self.logger.error(f"Access forbidden for {user_id}")
            results.append({"id": user_id, "error": "forbidden", "depth": depth})
            return results
        
        if response.status == 429:
            self.rate_limited_count += 1
            self.logger.warning(
                f"Rate limited for {user_id} at depth {depth} (#{self.rate_limited_count}). "
                f"Scrapy will retry automatically."
            )
            return results
        
        # Parse successful response
        try:
            data = response.json()
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON for {user_id}")
            results.append({"id": user_id, "error": "json_decode_error", "depth": depth})
            return results
        
        profiles = data.get("profiles", [])
        
        # Extract follower IDs
        follower_ids = []
        for profile in profiles:
            uri = profile.get("uri", "")
            if uri.startswith("spotify:user:"):
                fid = uri.split(":")[-1]
                follower_ids.append(fid)
        
        follower_count = len(follower_ids)
        
        # Yield user data
        self.users_scraped += 1
        
        # Reset consecutive rate limits on success
        self.consecutive_rate_limits = 0
        
        results.append({
            "id": user_id,
            "depth": depth,
            "followers_count": follower_count,
            "follower_list": follower_ids,
            "profiles": profiles
        })
        
        self.logger.info(
            f"[{self.users_scraped}] Scraped {user_id} at depth {depth}: {follower_count} followers "
            f"(Rate limited: {self.rate_limited_count} times)"
        )
        
        # Continue BFS if not at max depth and not too many followers
        if depth < self.max_depth and follower_count <= self.max_followers:
            self.logger.debug(f"Creating {follower_count} child requests at depth {depth + 1} for {user_id}")
            for fid in follower_ids:
                req = self.create_follower_request(fid, depth + 1)
                if req:
                    results.append(req)
        else:
            if depth >= self.max_depth:
                self.logger.debug(f"Stopping BFS for {user_id} - reached max depth (depth={depth}, max={self.max_depth})")
            elif follower_count > self.max_followers:
                self.logger.debug(f"Stopping BFS for {user_id} - too many followers ({follower_count} > {self.max_followers})")
        
        # Only generate more tokens if we're running low AND have pending requests or active scraping
        if len(self.tokens) < 3 and self.tokens_being_generated == 0 and len(self.pending_requests) > 0:
            self.logger.info("Running low on tokens with pending requests, generating more")
            results.append(self.create_token_request())
        
        # Process any pending requests
        results.extend(self.process_pending_requests())
        return results

    def errback_followers(self, failure):
        """Handle follower request failures"""
        request = failure.request
        user_id = request.meta.get("user_id")
        depth = request.meta.get("depth")
        
        self.logger.error(f"Request failed for {user_id}: {failure}")
        
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

    def add_token(self, authorization: str, client_token: str):
        """Manually add a token to the pool (for testing/bootstrapping)"""
        headers = {
            "authorization": authorization,
            "client-token": client_token,
            "Accept": "application/json",
            "app-platform": "WebPlayer",
            "Referer": "https://open.spotify.com/",
            "Origin": "https://open.spotify.com",
        }
        token = SpotifyToken(headers)
        if token not in [t for t in self.tokens]:
            self.tokens.append(token)
            self.logger.info(f"Added token to pool. Total: {len(self.tokens)}")


# For manual token injection
# You can run this with:
# scrapy crawl spotify_graph -a start_user=l0renzz -a depth=2
