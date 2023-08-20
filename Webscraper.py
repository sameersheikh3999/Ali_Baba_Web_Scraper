from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import concurrent.futures
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import requests
import csv
import re
import requests
from bs4 import BeautifulSoup as bs
import os
import json
import time
from time import sleep
from datetime import datetime
import threading
import asyncio


def extract_hrefs(url):
    # Set up Chrome web driver
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager(version='114.0.5735.90').install()),options=chrome_options)
    driver.get(url)

    # Wait for the page to load
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "product-image")))

    # Find all product image links
    product_links = driver.find_elements(By.CLASS_NAME, "product-image")
    hrefs = [link.get_attribute("href") for link in product_links]

    # Close the driver
    driver.quit()

    return hrefs
# Define the keyword pattern for attribute names
keyword_pattern = re.compile(r'Color|Print|Material|Size', re.IGNORECASE)
chrome_prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.media_stream": 2,
        }
chrome_options = Options()
chrome_options.add_experimental_option("prefs", chrome_prefs)
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-logging")
driver = webdriver.Chrome(service=Service(ChromeDriverManager(version='114.0.5735.90').install()),options=chrome_options)

# Define the number of pages to extract hrefs from
num_runs = int(input("Enter the number of times you want to run the scraping: "))
all_runs_data = []
for run in range(num_runs):
    print(f"Run {run + 1} out of {num_runs}")
    nme = input("Enter the urlname: ")
    pg = int(input("Pages to Scrape: "))
    num_pages = pg
    base_url = f"https://www.alibaba.com/showroom/{nme}_{{}}.html"
    pgurls = [base_url.format(page) for page in range(1, num_pages + 1)]

    # Get keywords from the user
    keywords_input = input("Enter keywords separated by commas: ")
    keywords = [keyword.strip().lower() for keyword in keywords_input.split(",")]
    all_runs_data.append((nme, pg, keywords))

# start_time = time.time()
# Extract hrefs from multiple pages concurrently


# print(urls)

# URLs to scrape
# urls = pd.read_csv('product_hrefs.csv')['Href']

async def experience(soup):
    try:
        experience = soup.find('a', class_="verify-info")
        if experience:
            years = experience.text.strip("YRS")
        else:
            years = "0"

        if years == "0":
            div_elements = soup.find_all('div', class_='company-year')
            if div_elements:
                years = div_elements[0].text.strip("YRS")
            else:
                years = "0"

        return years
    except:
        return "0"
    
async def customize(soup):
    try:
        customize = soup.find("div", class_="custom-item")
        customize_text = customize.text.strip("Customized logo")
        customize_logo = "Yes " + customize_text 
        return customize_logo
    except:
        return "0"

async def lead_time(soup):
    try:
        lead_time_element = soup.find('td', string='Lead time (days)')
        if lead_time_element:
            lead_time_value = lead_time_element.find_next_sibling('td').get_text()
        lead_time_text = lead_time_value + " Days"
        return lead_time_text
    except:
        return "0"

async def min_price_text(soup):
    try:
        custom_items = soup.find_all("div", class_="custom-item")
        for item in custom_items:
            span = item.find("span", string="Customized logo")
            if span:
                min_order_text = item.find("span", string=lambda text: "Min. order" in text).text
        return min_order_text
            
    except:
        return 'N/A'

async def rating(soup):   
    try:
        rating_element = soup.find("div", class_="attr-content")
        ratings = rating_element.text.rstrip("/5")
        return ratings
    except: 
        return "N/A"

async def on_time(soup):
    try:
        on_time=soup.find_all("div", class_="attr-content")
        on_time_delivery=on_time[1].get_text().strip()
        return on_time_delivery
    except:
        return "N/A"


async def online_soup(soup):
    try:
        online=soup.find_all("div", class_="attr-content")
        online_revenue=online[3].get_text().strip()
        return online_revenue
    except:
        return "N/A"
    
async def start_order_button(sp):
    try:
        start_text= sp.find('div', class_='order-button')
        if start_text:
            start_order="Yes"
            return start_order
        else:
            start_order="No"
            return start_order
    except:
        return "N/A"

async def unit_price(soup):
    price_element = soup.find('span', class_='price')
    price_range = price_element.text.strip()
    
    if price_range:
        price_with_unit = price_range
        return price_with_unit
    else:
        price_with_unit=" "
        return price_with_unit

async def tags_data(sp):
    try:
        tags = sp.find('span', class_="hot-sale").text.strip()
        if tags:
            return tags
        else:
            return "N/A"
    except:
        return "N/A"


async def product_rating_data(sp):
    try:  
        product_rating = sp.find('span', class_="next-form-text-align review-value").text.strip()
        if product_rating:
            return product_rating
        else:
            return "N/A"
    except:
        return "N/A"

async def quantity_sold_data(sp):
    try:
        buyers = sp.find('span', class_='quantity-sold').text.strip()
        if buyers:
            return buyers
        else:
            return "N/A"
    except:
        return "N/A"

async def review_text_data(sp):
    try:
        review_text = sp.find('span', class_='next-form-text-align', string=lambda text: 'Reviews' in text).find_previous('span', class_='next-form-text-align review-value').find_next('span', class_='next-form-text-align').text.strip()
        if review_text:
            return review_text
        else:
            return "N/A"
    except:
        return "N/A"
    
async def img_src_link(soup):
    try:
        main_item_divs = soup.find_all('div', class_='main-item')
        if len(main_item_divs) >= 2:
            # Get the second div's image source link
            second_div = main_item_divs[1]
            img_element = second_div.find('img')
        
        if img_element:
            img_src = img_element['src']
            img_src=img_src.rsplit('_', 1)[0]
            img_src_formula= f'=IMAGE("{img_src}","ALT Text")'
            return img_src_formula
        else:
            return "N/A"
    except:
        return "N/A"
    
async def customization(sp):
    try:
        cust_item=sp.find('div', class_="3d-customization-item")
        if cust_item:
            return "Yes"
        else:
            return "No"
    except:
        return "No"



async def color(sp):
    try:
        Color = []
        Print = []
        Material = []
        Size = []

        entry_items = sp.find_all("dl", class_="do-entry-item")

        for entry_item in entry_items:
            # Find the attribute name (title) element within each entry_item
            attr_name = entry_item.find(class_='attr-name')
            if attr_name and keyword_pattern.search(attr_name.get('title', '')):
                # If the attribute name contains any of the keywords, extract the value
                value_element = entry_item.find(class_='do-entry-item-val')
                value = value_element.text.strip() if value_element else ''

                # Store the data in the appropriate list based on the keyword
                if 'Color' in attr_name.text:
                    Color.append(value)
                elif 'Print' in attr_name.text:
                    Print.append(value)
                elif 'Material' in attr_name.text:
                    Material.append(value)
                elif 'Size' in attr_name.text:
                    Size.append(value)

        return Color, Print, Material, Size
    except Exception as e:
        print("Error while extracting attributes:", e)
        return [], [], [], []

async def quantity_levels(soup, max_levels):
    try:
        quantity_levels = soup.find_all("div", class_="quality")
        data = []
        for i in range(min(len(quantity_levels), max_levels)):
            quantity_text = quantity_levels[i].text.strip()
            price_text = soup.find_all("div", class_="price")[i].find("span").text.strip()
            data.append((f"{quantity_text}: {price_text}",))
        return data
    except:
        return []

async def scrape_data(url,driver, max_levels=6):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        
        driver.get(url)
        r=driver.page_source
        sp=BeautifulSoup(r,'html.parser')
        
        

        # Extract listing text
        listing_text = soup.find("h1").text

        # Extract listing link
        listing_link = url

        # Verify if the target image is present
        target_img_src = "https://img.alicdn.com/imgextra/i1/O1CN01AOhmtZ1HQ08UWY7sf_!!6000000000751-2-tps-266-54.png_240x240.jpg"
        img_tags = soup.find_all("img")
        verified = "Yes" if any(tag.get("src") == target_img_src for tag in img_tags) else "No"

        # Extract color, print, material, and size
        Color, Print, Material, Size = await color(sp)
        years=await experience(soup)
        customize_logo=await customize(soup)
        lead_time_text=await lead_time(soup)
        min_order_price=await min_price_text(soup)
        ratings=await rating(soup)
        on_time_delivery=await on_time(soup)
        online_revenue=await online_soup(soup)
        start_order=await start_order_button(sp)
        price_with_unit=await unit_price(soup)
        product_rating=await product_rating_data(sp)
        buyers=await quantity_sold_data(sp)
        review_text=await review_text_data(sp)
        tags=await tags_data(sp)
        img_src_formula=await img_src_link(soup)
        cust_item=await customization(sp)

        # Extract quantity levels
        quantity_levels_result = await quantity_levels(soup, max_levels)
        
        # if verified=="No" or buyers=='N/A' or start_order=="No":
        #     return None, None, None , None, None, None, None, None, None, None, None, None, None, None, None, None, None, None ,None , None, None , None , *([None] * max_levels)

        return listing_text, listing_link, verified, Color, Print, Material, Size, years, customize_logo, lead_time_text, min_order_price, ratings, on_time_delivery, online_revenue, start_order, buyers,product_rating,review_text, tags, price_with_unit, img_src_formula,cust_item, *quantity_levels_result
    except Exception as e:
        print("Error while scraping:", e)
        return None, None, None , None, None, None, None, None, None, None, None, None, None, None, None, None, None, None ,None , None, None , None , *([None] * max_levels)


async def scrape_data_batch(urls_batch, driver, max_levels=6):
    batch_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls_batch)) as executor:
        loop = asyncio.get_event_loop()
        scrape_tasks = [loop.run_in_executor(executor, scrape_data, url, driver, max_levels) for url in urls_batch]
        batch_results = await asyncio.gather(*scrape_tasks)
    return batch_results

async def main():
    start_time = time.time() 
    try:
        for run_data in all_runs_data:
                nme, pg, keywords = run_data
                num_pages = pg
                base_url = f"https://www.alibaba.com/showroom/{nme}_{{}}.html"
                pgurls = [base_url.format(page) for page in range(1, num_pages + 1)]
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    hrefs_list = list(executor.map(extract_hrefs, pgurls))

                # Flatten the list of lists
                hrefs = [href for href_list in hrefs_list for href in href_list]

                # Check keywords (case-insensitive) and save matching hrefs to a CSV file
                matched_hrefs = set()
                for href in hrefs:
                    lowercase_href = href.lower()
                    if any(keyword in lowercase_href for keyword in keywords):
                        matched_hrefs.add(href)

                matched_hrefs_list=list(matched_hrefs)
                urls = [', '.join([url]) for url in matched_hrefs_list]

                
                batch_size = 10
                batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]

                # Set up Chrome options for the webdriver
                # chrome_prefs = {
                #     "profile.managed_default_content_settings.images": 2,
                #     "profile.managed_default_content_settings.media_stream": 2,
                # }
                # chrome_options = Options()
                # chrome_options.add_experimental_option("prefs", chrome_prefs)
                # chrome_options.add_argument("--headless")

                # # Initialize webdriver with ChromeDriverManager
                # driver_options = {
                #     "service": Service(ChromeDriverManager(version='114.0.5735.90').install()),
                #     "options": chrome_options
                # }

                # Define fieldnames for the CSV file
                fieldnames = ['Listing Text', 'Listing Link', 'Verified', 'Color', 'Print', 'Material', 'Size', 'Years', 'Customize Logo', 'Lead Days', 'Min Order', 'Vendor Rating', 'On-time delivery rate', 'Online revenue', 'Start Order Button', 'Buyer', 'Product Rating', 'Reviews', 'Tags', 'Unit Price', 'Img url','Customization']
                max_levels = 6
                fieldnames.extend([f"Quantity Level {i}" for i in range(1, max_levels + 1)])
                filename=nme+".csv"
                # Open CSV file
                with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(fieldnames)

                    for batch in batches:
                        batch_results = await scrape_data_batch(batch, driver, max_levels)
                        for result in batch_results:
                            if result is None:
                                continue  # Skip this result
                            unpacked_result = await result
                            writer.writerow(unpacked_result)

        end_time = time.time()
        duration = end_time - start_time
        print(f"Scraping completed in {duration:.2f} seconds.")

    finally:
        driver.quit()

# Run the main function
asyncio.run(main())
