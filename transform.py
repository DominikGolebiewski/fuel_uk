import duckdb
import pandas as pd


# Function to load data from DuckDB
def load_data():
    con = duckdb.connect(':memory:')
    query = """
        SELECT
            retailer,
            address,
            brand,
            latitude,
            longitude,
            postcode,
            b7,
            sdv,
            e5,
            e10,
            site_id
        FROM read_parquet(['data/raw/*.parquet'])
        limit 10
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

df = load_data()

# save to json file
# df.to_json('data/asda/asda_2022-02-21_10-21-00.json', orient='records')

print(df)