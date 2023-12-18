from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from lxml import etree
import numpy as np
import pandas as pd
import re
import json
import os
import boto3

options = Options()
options.add_argument('--incognito')
options.add_argument('--headless')
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
options.add_argument('user-agent={0}'.format(user_agent))
chrome_prefs = {}
options.experimental_options["prefs"] = chrome_prefs
chrome_prefs["profile.default_content_settings"] = {"images": 2}
driver = webdriver.Chrome(options = options)

int_idx_array = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])

def property_page_scrape():
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    try:
        property_name = soup.find(name = 'h1', id = 'propertyName').text.strip()
    except:
        property_name = np.nan
    dom = etree.HTML(str(soup))
    try:
        street = dom.xpath('//*[@id="propertyAddressRow"]/div[@class="propertyAddressContainer"]/h2/span[@class="delivery-address"]/span')[0].text
    except:
        street = np.nan
    try:
        city = dom.xpath('//*[@id="propertyAddressRow"]/div[@class="propertyAddressContainer"]/h2/span[not(@*)]')[0].text
    except:
        city = np.nan
    try:
        state = dom.xpath('//*[@id="propertyAddressRow"]/div[@class="propertyAddressContainer"]/h2/span[@class="stateZipContainer"]/span[1]')[0].text
    except:
        state = np.nan
    try:
        zipcode = dom.xpath('//*[@id="propertyAddressRow"]/div[@class="propertyAddressContainer"]/h2/span[@class="stateZipContainer"]/span[2]')[0].text
    except:
        zipcode = np.nan
    try:
        neighborhood_address = \
        dom.xpath('//*[@id="propertyAddressRow"]/div[@class="propertyAddressContainer"]/h2/span[@class="neighborhoodAddress"]/a')[0].text
    except:
        neighborhood_address = np.nan
    try:
        monthly_rent = dom.xpath('//*[@id="priceBedBathAreaInfoWrapper"]/div/div/ul/li[1]/div/p[2]')[0].text.replace('$', '\$')
    except:
        monthly_rent = np.nan
    try:
        bedrooms = dom.xpath('//*[@id="priceBedBathAreaInfoWrapper"]/div/div/ul/li[2]/div/p[2]')[0].text
    except:
        bedrooms = np.nan
    try:
        bathrooms = dom.xpath('//*[@id="priceBedBathAreaInfoWrapper"]/div/div/ul/li[3]/div/p[2]')[0].text
    except:
        bathrooms = np.nan
    try:
        square_feet = dom.xpath('//*[@id="priceBedBathAreaInfoWrapper"]/div/div/ul/li[4]/div/p[2]')[0].text
    except:
        square_feet = np.nan
    try:
        amenities = tuple(map(lambda iter: iter.text, dom.xpath('//*[@id="amenitiesSection"]/div/div[@class = "spec"]//ul/li/span')))
    except:
        amenities = np.nan
    try:
        units = tuple(map(lambda iter: int(re.search('\d+', iter.text).group(0)), \
                          dom.xpath('//*[@id="pricingView"]/div[2]/div/div[1]/div[@class = "availability"]')))
        total_units = sum(units) if len(units) > 0 else np.nan
    except:
        total_units = np.nan
    try:
        contact_number = dom.xpath('//*[@id="propertyHeader"]/div[2]/div[2]/span')[0].text
    except:
        contact_number = np.nan
    row = np.array([[property_name, street, city, state, zipcode, neighborhood_address, \
               monthly_rent, bedrooms, bathrooms, square_feet, amenities, total_units, contact_number]])
    df_row = pd.DataFrame(row, columns = ('Property Name', 'Street', 'City', 'State', 'ZIP Code', 'Neighborhood Address', 'Monthly Rent', \
                                          'Bedrooms', 'Bathrooms', 'Square Feet', 'Amenities', 'Total Units', 'Contact Number'))
    return(df_row)

def get_property_listings_single_location(location):
    list_listings = []
    driver.get("https://www.apartments.com/")
    time.sleep(8)
    search_box = driver.find_element(By.XPATH, '//*[@id="quickSearchLookup"]')
    search_box.send_keys(location)
    time.sleep(3)
    search_button = driver.find_element(By.XPATH, '//*[@id="quickSearch"]/div/fieldset/div/button')
    search_button.click()
    time.sleep(8)
    more_options_button = driver.find_element(By.XPATH, '//*[@id="advancedFiltersIcon"]')
    more_options_button.click()
    time.sleep(3)
    apartments_checkbox = driver.find_element(By.XPATH, '//*[@id="PropertyType_1"]')
    apartments_checkbox.click()
    time.sleep(5)
    more_options_exit_button = driver.find_element(By.XPATH, '//*[@id="advancedFilters"]/section/button[@class="btn btn-sm btn-primary done"]')
    more_options_exit_button.click()
    time.sleep(8)
    pages = driver.find_elements(By.XPATH, "//*[@id='paging']/ol/li/a")
    num_pages = len(pages)
    if(num_pages == 0):
        num_pages = 1
    listings = set()
    for i in range(num_pages):
        listings = \
        listings.union(set(map(lambda x: x.get_attribute("href"), driver.find_elements(By.XPATH, "//*[@id='placardContainer']/ul/li[@class='mortar-wrapper']/article/section/div/div[@class='property-info']//a[@class='property-link']"))))
        if i + 1 < num_pages:
            pages = driver.find_elements(By.XPATH, "//*[@id='paging']/ol/li/a")
            pages[i + 1].click()
            time.sleep(8)
    for i in listings:
        driver.get(i)
        time.sleep(5)
        list_listings.append(property_page_scrape())
    df_listings = pd.concat(list_listings)
    df_listings.reset_index(drop = True)
    return(df_listings)

def get_property_listings_multiple_locations(locations):
    list_dfs = []
    for location in locations:
        list_dfs.append(get_property_listings_single_location(location))
    df_listings_mult_loc = pd.concat(list_dfs)
    df_listings_mult_loc.reset_index(drop = True)
    return(df_listings_mult_loc)

s3 = boto3.client('s3')
s3.download_file('web-scraping-sink', 'config/locations.txt', 'locations.txt')

file = open('locations.txt', 'r')
content = file.readlines()[int_idx_array]
file.close()

locations = list(map(lambda s: s.strip(), content.split('|')))
df = get_property_listings_multiple_locations(locations)
driver.close()
df.to_csv(f'{int_idx_array}.csv', index = False)
s3.upload_file(f'{int_idx_array}.csv', 'web-scraping-sink', f'data/{int_idx_array}.csv')
