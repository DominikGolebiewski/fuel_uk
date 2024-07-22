import duckdb as du

def model(dbt, session):
    dbt.config(materialized="table")
    query = """
        SELECT
            retailer,
            last_updated,
            stations.address,
            stations.brand,
            stations.location.latitude,
            stations.location.longitude,
            stations.postcode,
            stations -> 'prices' -> 'B7' -> double as b7,
            stations -> 'prices' -> 'SDV' -> double as sdv,
            stations -> 'prices' -> 'E5' -> double as e5,
            stations -> 'prices' -> 'E10' -> dourble as e10,
            stations.site_id
        FROM read_parquet('data/raw/*.parquet')
        limit 10
    """
    session.execute(query)
    df = session.fetchdf()