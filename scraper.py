import json
import sys
import re
from typing import List

import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from enum import Enum
from datetime import datetime
import time

import threading
import queue
from pydantic import BaseModel, Field

class userData(BaseModel):
    id: str = ""
    scraping_depth: int = 0
    name: str = "Unknown"
    playlists: int = 0
    followers: int = 0
    following: int = 0
    follower_list: List[str] = Field(default_factory=list)
    error: str = ""

class queueItem(BaseModel):
    id: str
    depth: int
    retries: int = 0
    error: str = ""

MAX_THREADS = 10
TIMEOUT = 15
QUEUE_TIMEOUT = 5
MAX_FOLLOWERS = 100
DEBUG = True
MAX_RETRIES = 2

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]

user_id_pattern = re.compile(r'^(.*:)(.*?)-[^-]*$')
user_id_string = '[id^="card-title-spotify:user:"]'
user_name_class_string = '.__NC_butOiOksXo2E3M1'
user_details_class_string = '.JWDnag2Mepdf9QE0cNbg'


visited_users = {}
user_queue = queue.Queue()
data_queue = queue.Queue()
visited_users = set()
visited_lock = threading.Lock()
driver_init_lock = threading.Lock()

error_counter_lock = threading.Lock()
error_counter = {}
error_users_lock = threading.Lock()
error_users: List[queueItem] = []

class ScrapingMessages(Enum):
    SCRAPING_LIMIT_REACHED = "scraping_limit_reached"
    TIMEOUT = "timeout"
    USER_NOT_FOUND = "user_not_found"
    USERNAME_NOT_FOUND = "username_not_found"
    USER_DETAILS_NOT_FOUND = "user_details_not_found"
    NO_FOLLOWERS = "no_followers"
    NO_FOLLOWERS_IN_FOLLOWER_PAGE = "no_followers_in_follower_page"
    FOLLOWERS_NOT_MATCHING = "followers_not_matching"
    ERROR_DURING_FOLLOWER_SCRAPE = "error_during_follower_scrape"
    TOO_MANY_FOLLOWERS = "too_many_followers"
    NETWORK_ERROR = "network_error"




class UserNotFoundError(Exception):
    def __init__(self, message):
        super().__init__(message)
        if DEBUG:
            with error_counter_lock:
                if message in error_counter:
                    error_counter[message] += 1
                else:
                    error_counter[message] = 1
        
        if DEBUG:
            print(f"[ScrapingException] {message} (count: {error_counter[message]})")


class ScrapingException(Exception):
    def __init__(self, message):
        super().__init__(message)
        if DEBUG:
            with error_counter_lock:
                if message in error_counter:
                    error_counter[message] += 1
                else:
                    error_counter[message] = 1
        
        if DEBUG:
            print(f"[ScrapingException] {message} (count: {error_counter[message]})")


def get_chrome_options(user_agent=None):
    if False:
        options = Options()
        # options.add_argument("--headless") # Handled by uc.Chrome(headless=True)
        options.page_load_strategy = 'normal'
        options.add_argument("--disable-dev-shm-usage")
        # options.add_experimental_option("excludeSwitches", ["enable-automation"]) # Handled by uc
        # options.add_argument("--disable-blink-features=AutomationControlled") # Handled by uc
    else:
        options = Options()
        options.add_argument("--headless")
        options.page_load_strategy = 'eager'
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("--disable-blink-features=AutomationControlled")

    if user_agent:
        options.add_argument(f"--user-agent={user_agent}")
    else:
        options.add_argument(
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/141.0.7390.65 Safari/537.36"
        )
    return options


def start_webdriver(user_agent=None):
    options = get_chrome_options(user_agent)
    if False:
        with driver_init_lock:
            driver = uc.Chrome(options=options, headless=True)
    else:
        driver = webdriver.Chrome(options=options)
    return driver


def get_user_details(user: userData, driver: webdriver.Chrome):
    driver.get(f"https://open.spotify.com/user/{user.id}")
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located(
                (By.XPATH, 
                    f"//*[contains(normalize-space(text()), 'Follow') or contains(normalize-space(text()), 'Follower')]")
            )
        )
    except:
        raise ScrapingException(ScrapingMessages.USER_NOT_FOUND.value)

    try:
        user.name = driver.find_element(
            By.CSS_SELECTOR, user_name_class_string).text
    except:
        raise ScrapingException(ScrapingMessages.USERNAME_NOT_FOUND.value)

    try:
        selection =  driver.find_element(By.CSS_SELECTOR, user_details_class_string).text
        pl_match = re.search(r'(\d+(?:,\d+)*)\s+Public Playlist', selection, re.IGNORECASE)
        fo_match = re.search(r'(\d+(?:,\d+)*)\s+Follower', selection, re.IGNORECASE)
        fi_match = re.search(r'(\d+(?:,\d+)*)\s+Following', selection, re.IGNORECASE)

        user.playlists = int(pl_match.group(1).replace(',', '')) if pl_match else 0
        user.followers = int(fo_match.group(1).replace(',', '')) if fo_match else 0
        user.following = int(fi_match.group(1).replace(',', '')) if fi_match else 0
    except:
        raise ScrapingException(
            ScrapingMessages.USER_DETAILS_NOT_FOUND.value)
    return


def find_users(user_id, driver):
    driver.get(f"https://open.spotify.com/user/{user_id}/followers")
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, user_id_string)
            )
        )
    except:
        raise ScrapingException(
            ScrapingMessages.NO_FOLLOWERS_IN_FOLLOWER_PAGE.value)

    elems = driver.find_elements(By.CSS_SELECTOR, user_id_string)
    user_ids = []
    try:
        for elem in elems:
            user_id = elem.get_attribute('id')
            match = user_id_pattern.match(user_id)
            if match:
                user_ids.append(match.group(2))
    except:
        raise ScrapingException(
            ScrapingMessages.ERROR_DURING_FOLLOWER_SCRAPE.value)

    return user_ids

def data_writer(output_filename: str):
    with open(output_filename, 'w', encoding='utf-8') as f:
        while True:
            item = data_queue.get()

            if item is None:
                data_queue.task_done()
                break

            f.write(item.model_dump_json() + "\n")
            f.flush()

            data_queue.task_done()

    print("[Writer] Finished writing to disk.")


def worker(thread_id):
    print(f"[Thread-{thread_id}] Starting browser...")

    user_agent = USER_AGENTS[thread_id % len(USER_AGENTS)]
    driver = start_webdriver(user_agent)
    try:
        while True:
            item = user_queue.get()
            if item is None:
                user_queue.task_done()
                break
            if item.retries > MAX_RETRIES:
                if DEBUG:
                    print(f"[Thread-{thread_id}] Max retries reached for user {item.id}, skipping.")
                user_queue.task_done()
                with error_users_lock:
                    error_users.append(item)
                with visited_lock:
                    visited_users.add(item.id)
                continue
            
            user = userData(id=item.id, depth=item.depth)

            with visited_lock:
                if user.id in visited_users:
                    user_queue.task_done()
                    continue
                visited_users.add(user.id)

            try:
                get_user_details(user, driver)
            except Exception as e:
                user.error = str(e)
                with visited_lock:
                    visited_users.remove(item.id)
                user_queue.put(queueItem(id=item.id, depth=item.depth, retries=item.retries + 1, error=user.error))  # requeue for retry

                user_queue.task_done()
                continue

            if user.followers <= 0 or user.followers > MAX_FOLLOWERS:
                user.error = ScrapingMessages.NO_FOLLOWERS.value if user.followers == 0 else ScrapingMessages.TOO_MANY_FOLLOWERS.value
                data_queue.put(user)
                user_queue.task_done()
                continue
            try:
                user.follower_list = find_users(user.id, driver)
                if user.followers != len(user.follower_list):
                    raise ScrapingException(ScrapingMessages.FOLLOWERS_NOT_MATCHING.value)
            except Exception as e:
                user.error = str(e)
                with visited_lock:
                    visited_users.remove(item.id)
                user_queue.put(queueItem(id=item.id, depth=item.depth, retries=item.retries + 1, error=user.error))  # requeue for retry

                user_queue.task_done()
                continue

            data_queue.put(user)
            if item.depth > 0:
                with visited_lock:
                    for user_id in user.follower_list:
                        if user_id not in visited_users:
                            user_queue.put(queueItem(id=user_id, depth=item.depth - 1))

            if DEBUG:
                print(f"[Thread-{thread_id}] Processed user {user.id} at depth {depth} with {len(user.follower_list)} followers scraped and {user.followers} followers found in details")
            user_queue.task_done()
    finally:
        driver.quit()
        print(f"[Thread-{thread_id}] Browser closed.")

def write_metadata(starttime, output_filename: str):
    endtime = time.perf_counter()
    scrape_duration = endtime - starttime
    metadata = {
        "total_users_scraped": len(visited_users),
        "total_errors": sum(error_counter.values()),
        "errors_by_type": dict(error_counter),
        "scrape_duration_seconds": round(scrape_duration, 3),
        "time_per_scrape": round(scrape_duration / len(visited_users), 3) if len(visited_users) > 0 else 0,
        "error_ratio": len(error_users) / len(visited_users) * 100 if len(visited_users) > 0 else 0,
        "error_users": [user.id for user in error_users],
    }
    if DEBUG:
        print(f"Scrape Duration: {scrape_duration:.2f} seconds")
        print(f"Time per Scrape: {scrape_duration / len(visited_users):.4f} seconds")
        print(f"Total Errors: {sum(error_counter.values())}")
        with error_counter_lock:
            for error_msg, count in error_counter.items():
                print(f"  {error_msg}: {count}")

        with error_users_lock:
            print(f"Users with errors: {len(error_users)}")
            print(f"Error Ratio: {len(error_users) / len(visited_users) * 100:.2f}%")
            for user in error_users:
                print(f"  {user.id}")

    with open(output_filename, 'a', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)
    return metadata


def start_scrape(startuser: str, depth: int, output_filename: str):
    if depth < 0:
        return
    starttime= time.perf_counter()
    worker_thread_count = MAX_THREADS - 1 if MAX_THREADS > 1 else 1  # one thread is for data writer
    user_queue.put(queueItem(id=startuser, depth=depth))

    writer_t = threading.Thread(target=data_writer, args=(output_filename,),)
    writer_t.start()

    worker_threads = []
    for i in range(worker_thread_count):  # one thread is for data
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        worker_threads.append(t)

    try:
        user_queue.join()
        for _ in range(worker_thread_count):
            user_queue.put(None)
    except KeyboardInterrupt:
        print("Main thread interrupted. Waiting for workers to finish...")
        while not user_queue.empty():
            user_queue.get_nowait()
            user_queue.task_done() 
        for _ in range(worker_thread_count):
            user_queue.put(None)
        for t in worker_threads:
           t.join()
        print(f"Data queue size: {data_queue.qsize()} - waiting for data writer to finish...")
        data_queue.join()
        data_queue.put(None)
        writer_t.join()
        print("Scraping interrupted by user.")
        sys.exit(0)


    for t in worker_threads:
        t.join()

    write_metadata(starttime, output_filename)

    data_queue.put(None)
    data_queue.join()
    writer_t.join()
    print(f"Scraping completed for {len(visited_users)} users, data saved")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Usage: python scraper.py <startuser> <depth> [output_filename]")
        sys.exit(1)
    startuser = sys.argv[1]
    depth = int(sys.argv[2])
    if len(sys.argv) >= 4:
        output_filename = sys.argv[3]
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_filename = f"spotify_user_graph_{startuser}_{depth}_{timestamp}.json"
    print(f"Starting scrape from user {startuser} with breath {depth}")
    print(f"Writing output to {output_filename}")
    start_scrape(startuser, depth, output_filename)
