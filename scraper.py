import sys
import re

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import rustworkx as rx
from enum import Enum
from datetime import datetime

timeout = 2


pattern = re.compile(r'^(.*:)(.*?)-[^-]*$')
user_id_string = '[id^="card-title-spotify:user:"]'
user_name_class_string = '.__NC_butOiOksXo2E3M1'
user_details_class_string = '.JWDnag2Mepdf9QE0cNbg'
class ScrapingError(Enum):
    SCRAPING_LIMIT_REACHED = "scraping_limit_reached"
    TIMEOUT = "timeout"
    USER_NOT_FOUND = "user_not_found"
    NO_FOLLOWERS = "no_followers"
    TOO_MANY_FOLLOWERS = "too_many_followers"
    NETWORK_ERROR = "network_error"


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

def get_user_details(user, driver):
    driver.get(f"https://open.spotify.com/user/{user}")
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(normalize-space(text()), 'Followers')]")
            )
        )
    except:
        return {}

    full_text = driver.page_source

    user_name = ""
    try:
        user_name = driver.find_element(By.CSS_SELECTOR, user_name_class_string).text
    except:
        return {}
    
    user_details = []
    try:
        user_details_element = driver.find_element(By.CSS_SELECTOR, user_details_class_string)
        child_elements = user_details_element.find_elements(By.XPATH, "./*")
    except:
        return {'name': user_name}
    playlists = [re.findall(r'\d+', elem.text) for elem in child_elements if elem.text.strip().lower().find("playlists") != -1]
    followers = [re.findall(r'\d+', elem.text) for elem in child_elements if elem.text.strip().lower().find("followers") != -1]
    following = [re.findall(r'\d+', elem.text) for elem in child_elements if elem.text.strip().lower().find("following") != -1]
    user_details = {
        "name": user_name,
        "playlists": int(playlists[0][0].replace(',', '')) if playlists else 0,
        "followers": int(followers[0][0].replace(',', '')) if followers else 0,
        "following": int(following[0][0].replace(',', '')) if following else 0,
    }

    return user_details




def find_users(user_id, driver):
    driver.get(f"https://open.spotify.com/user/{user_id}/followers")
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, user_id_string)
            )
        )
    except:
        return []

    elems = driver.find_elements(By.CSS_SELECTOR, user_id_string)
    user_ids = []

    for elem in elems:
        user_id = elem.get_attribute('id')
        match = pattern.match(user_id)
        if match:
            user_ids.append(match.group(2))

    return user_ids


def start_scrape(startuser, breath):
    if breath < 0:
        return
    user_to_follower_map = {}
    G = rx.PyDiGraph()
    
    options = get_chrome_options()
    driver = webdriver.Chrome(options=options)

    scrape(startuser, breath, G, user_to_follower_map, driver)

    driver.quit()

    print("Scraping complete.")
    print(f"Total users scraped: {len(user_to_follower_map)}")

    rx.write_graphml(G, f"spotify_user_graph_{startuser}_{breath}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.graphml")

    return user_to_follower_map, G

def scrape(user_id, breath, graph, visited_users, driver):
    if user_id in visited_users:
        return visited_users[user_id]
    user_details = get_user_details(user_id, driver)
    message = ""
    if user_details == {}:
        message = ScrapingError.USER_NOT_FOUND
        index = graph.add_node({'id': user_id, 'error': message})
        visited_users[user_id] = index
        return index
    elif user_details['followers'] > 100:
        message = ScrapingError.TOO_MANY_FOLLOWERS
    elif breath < 0:
        message = ScrapingError.SCRAPING_LIMIT_REACHED
    
    print(f"User {user_id} has {user_details['followers']} followers.")
    user = {
        'id': user_id,
        'name': user_details.get('name', 'Unknown'),
        'playlists': user_details.get('playlists', 0),
        'followers': user_details.get('followers', 0),
        'following': user_details.get('following', 0),
    }
    if message != "":
        user['error'] = message.value
        index = graph.add_node(user)
        visited_users[user_id] = index
        return index
    
    user_ids = find_users(user_id, driver)
    user['follower_list'] = ','.join(user_ids)

    index = graph.add_node(user)
    visited_users[user_id] = index

    for uid in user_ids:
        if uid not in visited_users:
            graph.add_edge(index, scrape(uid, breath - 1, graph, visited_users, driver), None)
        else:
            graph.add_edge(index, visited_users[uid], None)
    return index



# if True:
#     all_user_ids = start_scrape("jonas.f.rappold", 1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scraper.py <startuser> <breath>")
        sys.exit(1)
    startuser = sys.argv[1]
    breath = int(sys.argv[2])
    print(f"Starting scrape from user {startuser} with breath {breath}")
    print("Writing output to spotify_user_graph_<startuser>_<breath>_<timestamp>.graphml")
    all_user_ids, G = start_scrape(startuser, breath)
