import os
import json
import requests
from bs4 import BeautifulSoup
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO)

def get_dynamic_soup(url: str) -> BeautifulSoup:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        soup = BeautifulSoup(page.content(), "html.parser")
        browser.close()
        return soup

def clean_text(text):
    return text.replace('\u00a0', ' ').replace('\u2018', "'").replace('\u2019', "'").strip()

def load_progress(progress_file):
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            return int(f.read().strip())
    return 0

def save_progress(progress_file, index, lock):
    with lock:
        with open(progress_file, 'w') as f:
            f.write(str(index))

def append_to_file(file_name, product_details, lock):
    with lock:
        with open(file_name, 'a', encoding='utf-8') as f:
            f.write(json.dumps(product_details, ensure_ascii=False) + "\n")

def extract_price(text):
    match = re.search(r'\d+\.\d+', text)
    return match.group(0) if match else None

def process_product(index, product, output_file, progress_file, lock):
    product_details = {}
    try:
        soup = get_dynamic_soup(product['product_href'])

        # Extract product title
        title_tag = soup.select('h1.pdp-main-details__title')
        if title_tag:
            product_details['title'] = clean_text(title_tag[0].text)
        else:
            product_details['title'] = 'Unknown'

        # Extract prices
        price_tag = soup.select('strong.co-product__price.pdp-main-details__price')
        if price_tag:
            product_details['price'] = extract_price(price_tag[0].text)
        else:
            product_details['price'] = None

        was_price_tag = soup.select('span.co-product__was-price')
        if was_price_tag:
            product_details['was_price'] = extract_price(was_price_tag[0].text)
        else:
            product_details['was_price'] = None

        # Extract price per UOM
        price_per_uom_tag = soup.select('span.co-product__price-per-uom')
        if price_per_uom_tag:
            price_per_uom_text = price_per_uom_tag[0].text.strip('()')
            product_details['price_per_uom'] = clean_text(price_per_uom_text)

        # Extract product weight
        weight_tag = soup.find('div', class_='pdp-main-details__weight')
        if weight_tag:
            product_details['weight'] = clean_text(weight_tag.text)

        # Extract product code
        product_code_tag = soup.find('span', class_='pdp-main-details__product-code')
        if product_code_tag:
            product_code_match = re.search(r'\d+', product_code_tag.text)
            product_details['product_code'] = product_code_match.group(0) if product_code_match else None

        # Extract other details
        details_sections = soup.find_all('div', class_='pdp-description-reviews__product-details-cntr')
        for section in details_sections:
            section_title_tag = section.find('div', class_='pdp-description-reviews__product-details-title')
            section_content_tag = section.find('div', class_='pdp-description-reviews__product-details-content')
            if section_title_tag and section_content_tag:
                section_title = clean_text(section_title_tag.text).lower().replace(' ', '_')
                product_details[section_title] = clean_text(section_content_tag.text)

        # Extract product classifications from breadcrumbs
        breadcrumbs = soup.find('div', {'data-auto-id': 'pdpBreadcrumb'})
        if breadcrumbs:
            classification_keys = ['category', 'department', 'aisle', 'shelf']
            classifications = {}
            breadcrumb_links = breadcrumbs.find_all('a', class_='breadcrumb__link')
            for i, link in enumerate(breadcrumb_links):
                classification_name = clean_text(link.get_text(separator=" ", strip=True).replace('breadcrumb', ''))
                if i < len(classification_keys):
                    classifications[classification_keys[i]] = classification_name
            product_details['classifications'] = classifications

        # Add product_href to product_details
        product_details['href'] = product['product_href']

        append_to_file(output_file, product_details, lock)
        save_progress(progress_file, index + 1, lock)
        logging.info(f"Processed product {index + 1}: {product_details['title']}")
    except Exception as e:
        logging.error(f"Error processing product {index + 1}: {e}")
        save_progress(progress_file, index, lock)

def main():
    progress_file = "progress.txt"
    output_file = "offer_products_details.json"
    start_index = load_progress(progress_file)
    lock = Lock()

    if os.path.exists("offer_products.json"):
        products = json.load(open("offer_products.json"))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_product, index, product, output_file, progress_file, lock)
            for index, product in enumerate(products[start_index:], start=start_index)
        ]
        for future in as_completed(futures):
            future.result()  # This will raise any exceptions caught in the threads

if __name__ == "__main__":
    main()
