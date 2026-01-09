#!/usr/bin/env python3
"""
Run the Spotify graph scraper with Scrapy
"""
import sys
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import importlib.util

# Load the spider module
spec = importlib.util.spec_from_file_location("scraper_scrapy", "scraper_scrapy.py")
spider_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(spider_module)

SpotifyGraphSpider = spider_module.SpotifyGraphSpider

def run_scraper(start_user, depth=2, max_followers=100, output_file='output.jsonl', checkpoint_file=None, resume=False):
    # import debugpy
    # debugpy.listen(("0.0.0.0", 5678)) 
    # print("Waiting for debugger to attach...")
    # debugpy.wait_for_client() 
    
    if checkpoint_file is None:
        checkpoint_file = f'checkpoint_{start_user}.json'
    
    # Check if resuming from checkpoint
    checkpoint_data = None
    if resume:
        checkpoint_data = SpotifyGraphSpider.load_checkpoint(checkpoint_file)
        if checkpoint_data:
            print(f"Resuming from checkpoint: {checkpoint_file}")
            print(f"  - Previously visited users: {len(checkpoint_data.get('visited_users', []))}")
            print(f"  - Users in queue: {len(checkpoint_data.get('user_queue', []))}")
            # Use checkpoint parameters if not explicitly overridden
            if depth == 2:  # default value, use checkpoint's
                depth = checkpoint_data.get('max_depth', depth)
            if max_followers == 100:  # default value, use checkpoint's
                max_followers = checkpoint_data.get('max_followers', max_followers)
        else:
            print(f"Warning: No checkpoint file found at {checkpoint_file}, starting fresh")
            resume = False
    
    # When resuming, append to output file instead of overwriting
    feed_settings = {
        output_file: {
            "format": "jsonlines",
            "overwrite": not resume,  # Don't overwrite when resuming
        }
    }
    
    process = CrawlerProcess(settings={
        "FEEDS": feed_settings
    })
    
    # Pass checkpoint data as JSON string if resuming
    import json
    resume_data = json.dumps(checkpoint_data) if (resume and checkpoint_data) else None
    
    process.crawl(
        SpotifyGraphSpider,
        start_user=start_user,
        depth=str(depth),
        max_followers=str(max_followers),
        checkpoint_file=checkpoint_file,
        resume_data=resume_data,
    )
    process.start()

def resume_scraper(checkpoint_file, output_file=None):
    """Resume a scrape from a checkpoint file"""
    checkpoint_data = SpotifyGraphSpider.load_checkpoint(checkpoint_file)
    if not checkpoint_data:
        print(f"Error: Checkpoint file not found: {checkpoint_file}")
        sys.exit(1)
    
    start_user = checkpoint_data['start_user']
    depth = checkpoint_data['max_depth']
    max_followers = checkpoint_data['max_followers']
    
    if output_file is None:
        output_file = f'spotify_graph_{start_user}_{depth}.jsonl'
    
    print(f"Resuming Spotify scraper from checkpoint...")
    print(f"  Checkpoint: {checkpoint_file}")
    print(f"  Start user: {start_user}")
    print(f"  Depth: {depth}")
    print(f"  Max followers: {max_followers}")
    print(f"  Output: {output_file}")
    print(f"  Already visited: {len(checkpoint_data.get('visited_users', []))} users")
    print(f"  Queue size: {len(checkpoint_data.get('user_queue', []))} users")
    print()
    
    run_scraper(start_user, depth, max_followers, output_file, checkpoint_file, resume=True)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Spotify Graph Scraper')
    parser.add_argument('start_user', nargs='?', help='Spotify username to start scraping from')
    parser.add_argument('depth', nargs='?', type=int, default=2, help='Maximum depth to scrape (default: 2)')
    parser.add_argument('max_followers', nargs='?', type=int, default=100, help='Maximum followers to scrape per user (default: 100)')
    parser.add_argument('output_file', nargs='?', help='Output file (default: spotify_graph_<user>_<depth>.jsonl)')
    parser.add_argument('--resume', metavar='CHECKPOINT_FILE', help='Resume from a checkpoint file')
    
    args = parser.parse_args()
    
    if args.resume:
        # Resume mode
        resume_scraper(args.resume, args.output_file or (args.start_user if args.start_user and args.start_user.endswith('.jsonl') else None))
    elif args.start_user:
        # New scrape mode
        output_file = args.output_file or f'spotify_graph_{args.start_user}_{args.depth}.jsonl'
        
        print(f"Starting Spotify scraper...")
        print(f"  Start user: {args.start_user}")
        print(f"  Depth: {args.depth}")
        print(f"  Max followers: {args.max_followers}")
        print(f"  Output: {output_file}")
        print()
        
        run_scraper(args.start_user, args.depth, args.max_followers, output_file)
    else:
        parser.print_help()
        sys.exit(1)
