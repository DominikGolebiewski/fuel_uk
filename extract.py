import os
import pandas as pd
import json
import requests
import logging
from datetime import datetime
from slugify import slugify
from io import StringIO
import boto3
from botocore.exceptions import NoCredentialsError


# Setting up the logging configuration to append to the log file
logging.basicConfig(level=logging.INFO)

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

    # Function to upload a file to an S3 bucket
    def upload_to_s3(self, file_name):

        # Initialize the S3 client
        s3_client = boto3.client('s3')
        bucket = 'raw-petrol-data'

        try:
            # Upload the file
            s3_client.upload_file(file_name, bucket, file_name)
            print(f"File '{file_name}' uploaded to '{bucket}'.")
        except FileNotFoundError:
            print(f"The file '{file_name}' was not found.")
        except NoCredentialsError:
            print("Credentials not available.")

    def save_data_to_file(self):
        try:
            _updated_at = self.data.get('last_updated', datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
            self.logger.info(f'Last updated at: {_updated_at}')
            if isinstance(_updated_at, int):
                updated_at = datetime.fromtimestamp(_updated_at).strftime('%Y-%m-%d_%H-%M-%S')
            else:
                updated_at = datetime.strptime(_updated_at, '%d/%m/%Y %H:%M:%S').strftime('%d_%m_%Y_%H_%M_%S')

            self.logger.info(f'Updated at: {updated_at}')

            retailer_slug = slugify(str(self.retailer))
            raw_data_path = os.path.join('data/raw')
            os.makedirs(raw_data_path, exist_ok=True)
            
            filename = f'{retailer_slug}_{updated_at}.snappy.parquet'
            file_path = os.path.join(raw_data_path, filename)
            self.logger.info(f'Saving data to {file_path}')
            
            json_data = json.dumps(self.data)

            df = pd.read_json(StringIO(json_data))
            df_norm = pd.json_normalize(df['stations'])
            df = df.drop('stations', axis=1)
            df = pd.concat([df, df_norm], axis=1)
            df.rename(columns={
                'prices.E10': 'E10',
                'prices.B7': 'B7',
                'prices.E5': 'E5',
                'prices.SDV': 'SDV',
                'location.latitude': 'latitude',
                'location.longitude': 'longitude',
            }, inplace=True)

            expected_columns = ['E10', 'B7', 'E5', 'SDV']

            # Add missing price columns with null values
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None

            logging.info(df)
            # exit()
            # column prices has json data with petrol prices like b7 e5 e10 and sdv. I want all off them to be present but if they 
            # dont exists i want it to output null but the key must exists 


            # add self.retialer column to the dataframe
            df['retailer'] = self.retailer

            df.to_parquet(file_path, compression='snappy')
            self.upload_to_s3(file_path)
                
            self.logger.info(f'Successfully saved data to {file_path}')
        except Exception as e:
            self.logger.error(f'Error saving data for {self.retailer}: {e}')


def main():

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
    
    for entry in urls:
        retailer = entry['retailer']
        url = entry['url']
        fetcher = FuelPriceFetcher(retailer, url)
        fetcher.fetch_data()


if __name__ == '__main__':
    main()
