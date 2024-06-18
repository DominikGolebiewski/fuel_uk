import pandas as pd
import json
import requests

class FuelPriceFetcher:
    def __init__(self, retailer, url):
        self.retailer = retailer
        self.url = url
        self.data = None
        self.df = None

    def fetch_data(self):
        response = requests.get(self.url)
        response.raise_for_status()
        self.data = response.json()

    def process_data(self):
        if 'stations' in self.data:
            self.df = pd.json_normalize(self.data['stations'], sep='_')
            all_possible_prices = ['prices_B7', 'prices_E10', 'prices_E5']
            for price in all_possible_prices:
                if price not in self.df.columns:
                    self.df[price] = None
            self.df['last_updated'] = self.data['last_updated']
            self.df['retailer'] = self.retailer
        else:
            self.df = pd.DataFrame()

    def get_dataframe(self):
        self.fetch_data()
        self.process_data()
        return self.df

class FuelPricesAggregator:
    def __init__(self, urls):
        self.urls = urls
        self.dataframes = []

    def aggregate_data(self):
        for entry in self.urls:
            retailer = entry['retailer']
            url = entry['url']
            fetcher = FuelPriceFetcher(retailer, url)
            df = fetcher.get_dataframe()
            self.dataframes.append(df)
        combined_df = pd.concat(self.dataframes, ignore_index=True)
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

