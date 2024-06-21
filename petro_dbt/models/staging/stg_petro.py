import os
import pandas as pd
import json
import requests
import logging
from datetime import datetime
from slugify import slugify

# Setting up the logging configuration to append to the log file
logging.basicConfig(level=logging.INFO, filename='fuel_price_fetcher.log', 
                    filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class FuelPriceFetcher:

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }

    def __init__(self, retailer, url):
        self.retailer = retailer
        self.url = url
        self.data = None
        self.df = None
        self.logger = logging.getLogger(f'FuelPriceFetcher_{retailer}')

    def fetch_data(self):
        try:
            response = requests.get(self.url, headers=self.HEADERS)
            response.raise_for_status()
            self.data = response.json()
            self.save_data_to_file()
            self.logger.info(f'Successfully fetched data for {self.retailer}')
        except requests.exceptions.RequestException as e:
            self.logger.error(f'Error fetching data for {self.retailer}: {e}')
            self.data = {}

    def save_data_to_file(self):
        try:
            updated_at = self.data.get('last_updated', datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
            if isinstance(updated_at, int):
                updated_at = datetime.fromtimestamp(updated_at).strftime('%Y-%m-%d_%H-%M-%S')
            else:
                updated_at = datetime.strptime(updated_at, '%d/%m/%Y %H:%M:%S').strftime('%d_%m_%Y_%H_%M_%S')
            
            retailer_slug = slugify(self.retailer)
            retailer_folder = os.path.join('files', retailer_slug)
            os.makedirs(retailer_folder, exist_ok=True)
            
            filename = f'{retailer_slug}_{updated_at}.json'
            file_path = os.path.join(retailer_folder, filename)
            
            with open(file_path, 'w') as json_file:
                json.dump(self.data, json_file, indent=4)
                
            self.logger.info(f'Successfully saved data to {file_path}')
        except Exception as e:
            self.logger.error(f'Error saving data for {self.retailer}: {e}')

    def process_data(self):
        try:
            if 'stations' in self.data:
                self.df = pd.json_normalize(self.data['stations'], sep='_')
                all_possible_prices = ['prices_B7', 'prices_E10', 'prices_E5', 'prices_SDV']
                for price in all_possible_prices:
                    if price not in self.df.columns:
                        self.df[price] = None
                self.df['last_updated'] = self.data.get('last_updated')
                self.df['retailer'] = self.retailer
                self.logger.info(f'Successfully processed data for {self.retailer}')
            else:
                self.df = pd.DataFrame()
                self.logger.warning(f'No stations data found for {self.retailer}')
        except Exception as e:
            self.logger.error(f'Error processing data for {self.retailer}: {e}')
            self.df = pd.DataFrame()

    def get_dataframe(self):
        self.fetch_data()
        self.process_data()
        return self.df

class FuelPricesAggregator:
    def __init__(self, urls):
        self.urls = urls
        self.dataframes = []
        self.logger = logging.getLogger('FuelPricesAggregator')

    def aggregate_data(self):
        for entry in self.urls:
            retailer = entry['retailer']
            self.logger.info(f'Processing data for {retailer}')
            url = entry['url']
            self.logger.info(f'Fetching data from {url}')
            fetcher = FuelPriceFetcher(retailer, url)
            df = fetcher.get_dataframe()
            self.dataframes.append(df)
        try:
            combined_df = pd.concat(self.dataframes, ignore_index=True)
            self.logger.info('Successfully aggregated data from all retailers')
        except ValueError as e:
            self.logger.error(f'Error aggregating data: {e}')
            combined_df = pd.DataFrame()
        return combined_df

def model(dbt, session):
    dbt.config(materialized="table")
    urls = [
        {'retailer': 'Applegreen UK', 'url': 'https://applegreenstores.com/fuel-prices/data.json'},
        {'retailer': 'Ascona Group', 'url': 'https://fuelprices.asconagroup.co.uk/newfuel.json'},
        {'retailer': 'ASDA', 'url': 'https://storelocator.asda.com/fuel_prices_data.json'},
        {'retailer': 'BP', 'url': 'https://www.bp.com/en_gb/united-kingdom/home/fuelprices/fuel_prices_data.json'},
        {'retailer': 'Esso', 'url': 'https://fuelprices.esso.co.uk/latestdata.json'},
        {'retailer': 'JET Retail UK', 'url': 'https://jetlocal.co.uk/fuel_prices_data.json'},
        {'retailer': 'Morrisons', 'url': 'https://www.morrisons.com/fuel-prices/fuel.json'},
        {'retailer': 'Moto', 'url': 'https://moto-way.com/fuel-price/fuel_prices.json'},
        {'retailer': 'Motor Fuel Group', 'url': 'https://fuel.motorfuelgroup.com/fuel_prices_data.json'},
        {'retailer': 'Rontec', 'url': 'https://www.rontec-servicestations.co.uk/fuel-prices/data/fuel_prices_data.json'},
        {'retailer': 'Sainsburys', 'url': 'https://api.sainsburys.co.uk/v1/exports/latest/fuel_prices_data.json'},
        {'retailer': 'SGN', 'url': 'https://www.sgnretail.uk/files/data/SGN_daily_fuel_prices.json'},
        {'retailer': 'Shell', 'url': 'https://www.shell.co.uk/fuel-prices-data.html'},
        {'retailer': 'Tesco', 'url': 'https://www.tesco.com/fuel_prices/fuel_prices_data.json'},
    ]
    
    aggregator = FuelPricesAggregator(urls)
    combined_df = aggregator.aggregate_data()
    return combined_df
