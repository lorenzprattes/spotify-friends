import json
import sys
import re
from typing import List

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from enum import Enum
from datetime import datetime

import threading
import queue
from pydantic import BaseModel, Field

MAX_THREADS = 8
TIMEOUT = 2
QUEUE_TIMEOUT = 5
MAX_FOLLOWERS = 100
DEBUG = True

user_id_pattern = re.compile(r'^(.*:)(.*?)-[^-]*$')
user_id_string = '[id^="card-title-spotify:user:"]'
user_name_class_string = '.__NC_butOiOksXo2E3M1'

shutdown_event = threading.Event()
visited_users = {}
user_queue = queue.Queue()
data_queue = queue.Queue()
visited_users = set()
visited_lock = threading.Lock()


class ScrapingMessages(Enum):
    SCRAPING_LIMIT_REACHED = "scraping_limit_reached"
    TIMEOUT = "timeout"
    USER_NOT_FOUND = "user_not_found"
    USERNAME_NOT_FOUND = "username_not_found"
    USER_DETAILS_NOT_FOUND = "user_details_not_found"
    NO_FOLLOWERS_IN_DETAILS = "no_followers_in_details"
    NO_FOLLOWERS_IN_FOLLOWER_PAGE = "no_followers_in_follower_page"
    ERROR_DURING_FOLLOWER_SCRAPE = "error_during_follower_scrape"
    TOO_MANY_FOLLOWERS = "too_many_followers"
    NETWORK_ERROR = "network_error"


class userData(BaseModel):
    id: str = ""
    name: str = "Unknown"
    playlists: int = 0
    followers: int = 0
    following: int = 0
    follower_list: List[str] = Field(default_factory=list)
    error: str = ""


class UserNotFoundError(Exception):
    pass


class ScrapingException(Exception):
    pass


def get_chrome_options():
    options = Options()
    options.add_argument("--headless")
    options.page_load_strategy = 'normal'
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/141.0.7390.65 Safari/537.36"
    )
    return options


def start_webdriver():
    options = get_chrome_options()
    driver = webdriver.Chrome(options=options)
    return driver


def get_user_details(user: userData, driver: webdriver.Chrome):
    driver.get(f"https://open.spotify.com/user/{user.id}")
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 "//*[contains(normalize-space(text()), 'Followers')]")
            )
        )
    except:
        raise ScrapingException(ScrapingMessages.USER_NOT_FOUND.value)

    full_text = driver.page_source

    user_name = ""
    try:
        user.name = driver.find_element(
            By.CSS_SELECTOR, user_name_class_string).text
    except:
        raise ScrapingException(ScrapingMessages.USERNAME_NOT_FOUND.value)

    user_details = []
    try:
        full_text = driver.find_element(By.TAG_NAME, "body").text
        pl_match = re.search(r'(\d+(?:,\d+)*)\s+Public Playlist', full_text, re.IGNORECASE)
        fo_match = re.search(r'(\d+(?:,\d+)*)\s+Follower', full_text, re.IGNORECASE)
        fi_match = re.search(r'(\d+(?:,\d+)*)\s+Following', full_text, re.IGNORECASE)

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

    driver = start_webdriver()
    try:
        while True:
            item = user_queue.get()
            if item is None:
                user_queue.task_done()
                break

            id, depth = item
            user = userData(id=id)

            with visited_lock:
                if user.id in visited_users:
                    user_queue.task_done()
                    continue
                visited_users.add(user.id)

            try:
                user_details = get_user_details(user, driver)
            except Exception as e:
                user.error = str(e)
                data_queue.put(user)
                user_queue.task_done()
                continue

            if user.followers < 0 or user.followers > MAX_FOLLOWERS:
                user.error = ScrapingMessages.NO_FOLLOWERS_IN_DETAILS.value if user.followers == 0 else ScrapingMessages.TOO_MANY_FOLLOWERS.value
                data_queue.put(user)
                user_queue.task_done()
                continue
            try:
                user.follower_list = find_users(user.id, driver)
            except Exception as e:
                user.error = str(e)
                data_queue.put(user)
                user_queue.task_done()
                continue

            data_queue.put(user)
            if depth > 0:
                for user_id in user.follower_list:
                    with visited_lock:
                        if user_id not in visited_users:
                            user_queue.put((user_id, depth - 1))

            if DEBUG:
                print(f"[Thread-{thread_id}] Processed user {user.id} at depth {depth} with {len(user.follower_list)} followers scraped and {user.followers} followers found in details")
            user_queue.task_done()
    finally:
        driver.quit()
        print(f"[Thread-{thread_id}] Browser closed.")

def start_scrape(startuser: str, depth: int, output_filename: str):
    if depth < 0:
        return
    worker_thread_count = MAX_THREADS - 1 if MAX_THREADS > 1 else 1  # one thread is for data writer
    user_queue.put((startuser, depth))

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

    data_queue.put(None)
    data_queue.join()
    writer_t.join()
    print("Scraping complete, data saved")


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
