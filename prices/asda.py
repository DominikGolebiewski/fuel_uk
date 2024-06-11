import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import logging
from playwright.sync_api import sync_playwright


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# aisle_url = "https://groceries.asda.com/aisle/bakery/extra-special-bakery/view-all-extra-special-bakery/1215686354843-1215686354845-1215686354864"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'}

# Function to get the next page URL
def get_next_page_url(soup):
    next_page_tag = soup.find('a', {'data-auto-id': 'btnright'})
    if next_page_tag and 'href' in next_page_tag.attrs:
        return next_page_tag['href']
    return None

# Function to check if it is the last page
def is_last_page(soup):
    next_page_tag = soup.find('a', {'data-auto-id': 'btnright'})
    return next_page_tag and 'aria-disabled' in next_page_tag.attrs and next_page_tag['aria-disabled'] == 'true'


def get_dynamic_soup(url: str) -> BeautifulSoup:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        # page.wait_for_selector("div[data-module='Bakery - taxo btns + css']")  # Ensure the div is loaded
        soup = BeautifulSoup(page.content(), "html.parser")
        browser.close()
        return soup
    

def extract_links_and_categories(soup: BeautifulSoup):
    links = []
    div = soup.find("div", class_='taxonomy-explore')
    if div:
        a_tags = div.find_all("a", class_="asda-btn asda-btn--light taxonomy-explore__item")
        for a_tag in a_tags:
            href = a_tag.get("href")
            department = a_tag.get_text(strip=True)
            links.append({"href": href, "category": department})
    return links



def extract_categories(soup: BeautifulSoup):
    links = []
    div = soup.find("div", class_='taxonomy-explore')
    if div:
        a_tags = div.find_all("a", class_="asda-btn asda-btn--light taxonomy-explore__item")
        for a_tag in a_tags:
            href = a_tag.get("href")
            category = a_tag.get_text(strip=True)
            links.append({"href": href, "category": category})
    json.dump(links, open("asda_categories.json", "w"))
    return links

def extract_departments(soup: BeautifulSoup):
    links = []
    div = soup.find("div", class_='taxonomy-explore')
    if div:
        a_tags = div.find_all("a", class_="asda-btn asda-btn--light taxonomy-explore__item")
        for a_tag in a_tags:
            href = a_tag.get("href")
            department = a_tag.get_text(strip=True)
            links.append({"href": href, "department": department})
    return links

def extract_aisles(soup: BeautifulSoup):
    links = []
    div = soup.find("div", class_='taxonomy-explore')
    if div:
        a_tags = div.find_all("a", class_="asda-btn asda-btn--light taxonomy-explore__item")
        for a_tag in a_tags:
            href = a_tag.get("href")
            aisle = a_tag.get_text(strip=True)
            links.append({"href": href, "aisle": aisle})
    return links

groceries_url = "https://groceries.asda.com"
categories_url = "https://groceries.asda.com/cat/summer"


# Fetch the categories from the file if exists otherwise run above function
if os.path.exists("asda_categories.json"):
    cats = json.load(open("asda_categories.json"))
else:
    try:
        categories_soup = get_dynamic_soup(categories_url)
        categories = extract_categories(categories_soup)
    except Exception as e:
        logging.error(f"Error fetching region stores: {e}")

logging.info(f"Total categories: {cats}")

product_classes = []

# for category in cats:
#     print(category)

#     cat = {}
#     logging.info(f"Fetching category: {category['category']}")
#     cat['category'] = category['category']
#     cat['category_url'] = category['href']
#     try:
#         department_url = groceries_url + category['href']
#         logging.info(f"Fetching category URL: {department_url}")
#         department_soup = get_dynamic_soup(department_url)
#         departments = extract_categories(department_soup)
#         cat['departments'] = departments
#         logging.info(f"Total departments: {departments}")
#         product_classes.append(cat)
#     except Exception as e:
#         logging.error(f"Error fetching region stores: {e}")

# print(product_classes)


# try:
#     cat_soup = get_dynamic_soup(cat_url)
#     links_and_departments = extract_links_and_categories(cat_soup)
# except Exception as e:
#     logging.error(f"Error fetching region stores: {e}")

# products = []

# for link in links_and_departments[0:1]:
#     logging.info(f"Fetching category: {link['category']}")
#     category_link = groceries_url + link['href']
#     logging.info(f"Fetching category link: {category_link}")
#     department_soup = get_dynamic_soup(category_link)
#     departments_tag = department_soup.find('div', {'data-module': '240422_EVENTS_Summer_CustomNavigation_HTML'})
#     departments = departments_tag.find_all('a', class_='asda-btn asda-btn--light taxonomy-explore__item bannerLink')
#     for department in departments:
        
#         logging.info(f"Fetching department: {department.text.strip()}, URL: {department['href']}")

#         aisle_soup = get_dynamic_soup(department['href'])
#         aisle_tag = aisle_soup.find('div', {'data-module': '240520_EVENTS_SummerBBQ_CustomNavigation_HTML_CSS'})
#         aisles = aisle_tag.find('ul', class_='taxonomy-explore__list')

#         print(aisles)

#         exit()
#         for aisle in aisles[0:1]:
#             logging.info(f"Fetching aisle: {aisle.text.strip()}, URL: {aisle['href']}")

#             aisle_soup_pages = get_dynamic_soup(aisle['href'])

#             aisle_pagination = aisle_soup_pages.find('a', class_='co-pagination__last-page')
#             if aisle_pagination:
#                 aisle_pagination = aisle_pagination.text.strip()
#             else:
#                 aisle_pagination = 1
#             logging.info(f"Total pages: {aisle_pagination}")
            
#             for x in range(int(aisle_pagination)):
#                 aisle_pagination_url = aisle['href'] + f"?page={x+1}"
#                 logging.info(f"Fetching aisle pagination: {aisle_pagination_url}")
#                 product_soup = get_dynamic_soup(aisle_pagination_url)
#                 product_tags = product_soup.find_all('li', class_='co-item')

#                 for item in product_tags:
#                     product = {}
                    
#                     title_tag = item.find('h3', class_='co-product__title')
#                     logging.info(f"Fetching product: {title_tag.text.strip()}")
#                     if title_tag and title_tag.a:
#                         product['title'] = title_tag.a.text.strip()

#                     # Extract was price if available
#                     was_price_tag = item.find('span', class_='co-product__was-price')
#                     if was_price_tag:
#                         product['was_price'] = was_price_tag.text.strip()
#                     else:
#                         product['was_price'] = None

#                     # Extract product price
#                     price_tag = item.find('strong', class_='co-product__price')
#                     if price_tag:
#                         product['price'] = price_tag.text.strip()

#                     # Extract product volume
#                     volume_tag = item.find('span', class_='co-product__volume')
#                     if volume_tag:
#                         product['volume'] = volume_tag.text.strip()
                    
#                     saver_label_tag = item.find('div', class_='link-save-banner')
#                     if saver_label_tag:
#                         saver_label_text = saver_label_tag.get_text(separator=' ', strip=True)
#                         product['saver_label'] = saver_label_text
#                     else:
#                         product['saver_label'] = None

#                     # Extract promo rewards information
#                     promo_rewards_tag = item.find('div', class_='co-product__promo--rewards')
#                     if promo_rewards_tag:
#                         promo_rewards_text = promo_rewards_tag.get_text(separator=' ', strip=True)
#                         product['promo_rewards'] = promo_rewards_text
#                     else:
#                         product['promo_rewards'] = None

#                     product_href = item.find('a', class_='co-product__anchor')
#                     if product_href:
#                         product['product_href'] = groceries_url + product_href['href']
#                     else:
#                         product['product_href'] = None

#                     product['category'] = link['category']
#                     product['department'] = department.text.strip()
#                     product['aisle'] = aisle.text.strip()

#                     products.append(product)

            
# df = pd.DataFrame(products)
# df.to_csv('asda_products.csv', index=False)
# df.to_json('asda_products.json', orient='records', lines=True)