from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests


class HttpProcessor:
    def __init__(self, url):
        self.url = url

    def get(self, headers=None, params=None):
        response = requests.get(self.url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def post(self, data=None, headers=None, params=None):
        response = requests.post(self.url, data=data, headers=headers, params=params)
        response.raise_for_status()
        return response.json()


def my_post():
    url = "https://dmfw.mca.gov.cn/9095/stname/listPub"
    payload = {"stName": "New York"}

    response = requests.post(url, data=payload)
    if response.ok:
        data = response.json()
        return True, data
    else:
        return False, f'[ERROR] Request failed with status code, {response.status_code}'



def crawl_city():
    # List of city names
    city_list = ["New York", "London", "Tokyo"]

    # Create a webdriver instance and navigate to the website
    driver = webdriver.Chrome()
    driver.get("https://dmfw.mca.gov.cn/foreign.html")

    # Wait for the page to load
    time.sleep(2)

    # Loop through the city list and search for each city
    for city in city_list:
        # Find the search box and enter the city name
        search_box = driver.find_element(By.CLASS_NAME, "placeName")
        search_box.clear()
        search_box.send_keys(city)
        # search_btn.send_keys(Keys.ENTER) # enter is not useful
        search_btn = driver.find_element(By.XPATH, 'html/body/div[3]/div[2]/div[2]')
        search_btn.click()

        # Wait for the search results to load
        time.sleep(2)

        # Get the HTML source of the search results page
        page_source = driver.page_source

        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(page_source, "html.parser")

        # Extract the city information from the page
        city_info = soup.find("tbody", {"class": "list-content"})

        # Print the city information
        print(f"City: {city}\nInformation: {city_info}\n")

    # Close the webdriver instance
    driver.quit()


# crawl_city()
