#!/usr/bin/env python3
"""
Run the Spotify graph scraper with Scrapy
"""
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import importlib.util

# Load the spider module
spec = importlib.util.spec_from_file_location("scraper_scrapy", "scraper_scrapy.py")
spider_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(spider_module)

SpotifyGraphSpider = spider_module.SpotifyGraphSpider

def run_scraper(start_user, depth=2, max_followers=100, output_file='output.jsonl'):
    # import debugpy
    # debugpy.listen(("0.0.0.0", 5678)) 
    # print("Waiting for debugger to attach...")
    # debugpy.wait_for_client() 
    process = CrawlerProcess()
    process.crawl(
        SpotifyGraphSpider,
        start_user=start_user,
        depth=str(depth),
        max_followers=str(max_followers)
    )
    process.start()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python run_scraper.py <start_user> [depth] [max_followers] [output_file]")
        print("Example: python run_scraper.py l0renzz 2 100 output.jsonl")
        sys.exit(1)
    
    start_user = sys.argv[1]
    depth = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    max_followers = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    output_file = sys.argv[4] if len(sys.argv) > 4 else f'spotify_graph_{start_user}_{depth}.jsonl'
    
    print(f"Starting Spotify scraper...")
    print(f"  Start user: {start_user}")
    print(f"  Depth: {depth}")
    print(f"  Max followers: {max_followers}")
    print(f"  Output: {output_file}")
    print()
    
    run_scraper(start_user, depth, max_followers, output_file)
