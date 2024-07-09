import duckdb
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import folium_static
import matplotlib.cm as cm
import matplotlib.colors as colors
import altair as alt

# Function to load data from DuckDB
def load_data():
    con = duckdb.connect('petro_dbt/petro.duckdb')
    query = """
        SELECT * 
        FROM petro
        
        LEFT JOIN (SELECT retailer, MAX(last_updated) last_updated FROM petro GROUP BY 1) AS latest
            ON petro.retailer = latest.retailer AND petro.last_updated = latest.last_updated

        WHERE postcode != 'GX11 1AA'
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

# Function to assign colors based on price
def assign_colors(df, fuel_type):
    min_price = df[fuel_type].min()
    max_price = df[fuel_type].max()
    norm = colors.Normalize(vmin=min_price, vmax=max_price)
    colormap = cm.ScalarMappable(norm=norm, cmap='RdYlGn_r')  # Red to Green colormap, reversed
    
    df.loc[:, 'color'] = df[fuel_type].apply(lambda x: colors.to_hex(colormap.to_rgba(x)))
    return df

# Function to load historical data from DuckDB
def load_historical_data():
    con = duckdb.connect('petro_dbt/petro.duckdb')
    query = """
        SELECT retailer, last_updated, AVG(b7) AS b7, AVG(e10) AS e10, AVG(e5) AS e5, AVG(sdv) AS sdv 
        FROM petro
        GROUP BY retailer, last_updated
        ORDER BY last_updated DESC
    """
    historical_df = con.execute(query).fetchdf()
    con.close()
    return historical_df

# Load the data
df = load_data()
historical_df = load_historical_data()

# Define fuel types
fuel_types = ['b7', 'e10', 'e5', 'sdv']

# Convert relevant columns to numeric
for fuel_type in fuel_types:
    df[fuel_type] = pd.to_numeric(df[fuel_type], errors='coerce')
    historical_df[fuel_type] = pd.to_numeric(historical_df[fuel_type], errors='coerce')

# Extract unique retailers and brands
retailers = df['retailer'].unique()
brands = df['brand'].unique()

# Check and convert data types
df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
historical_df['last_updated'] = pd.to_datetime(historical_df['last_updated'], dayfirst=True)

# Streamlit sidebar for user selection
st.sidebar.title("Fuel Price Dashboard")
selected_fuel_type = st.sidebar.selectbox('Select Fuel Type', fuel_types)
selected_retailer = st.sidebar.selectbox('Select Retailer', retailers)

# Filter dataframe based on the selected fuel type and retailer
filtered_df = df[(~df[selected_fuel_type].isnull()) & (df['retailer'] == selected_retailer)]

# Assign colors to prices
filtered_df = assign_colors(filtered_df, selected_fuel_type)

# Overall Statistics
st.title("UK Fuel Price Analysis")
st.write("""
Analyze the latest fuel prices across various retailers in the UK. This dashboard provides insights into fuel pricing patterns and trends.
""")

min_price = df[selected_fuel_type].min()
max_price = df[selected_fuel_type].max()
mean_price = df[selected_fuel_type].mean()

min_price_row = df[df[selected_fuel_type] == min_price].iloc[0]
max_price_row = df[df[selected_fuel_type] == max_price].iloc[0]

st.header("Fuel Prices Overview")
st.write("### Key Statistics")
st.write(f"**Minimum Price:** {min_price} (Retailer: {min_price_row['retailer']}, Brand: {min_price_row['brand']}, Location: {min_price_row['address']})")
st.write(f"**Maximum Price:** {max_price} (Retailer: {max_price_row['retailer']}, Brand: {max_price_row['brand']}, Location: {max_price_row['address']})")
st.write(f"**Average Price:** {mean_price}")

# Map for min and max price locations
st.write("### Min and Max Price Locations")
min_max_map = folium.Map(location=[(min_price_row['latitude'] + max_price_row['latitude']) / 2, (min_price_row['longitude'] + max_price_row['longitude']) / 2], zoom_start=6, tiles='CartoDB Positron')

# Min price marker
folium.Marker(
    [min_price_row['latitude'], min_price_row['longitude']],
    popup=f"Min Price: {min_price}<br>Retailer: {min_price_row['retailer']}<br>Brand: {min_price_row['brand']}<br>Location: {min_price_row['address']}",
    tooltip="Min Price",
    icon=folium.Icon(color='green')
).add_to(min_max_map)

# Max price marker
folium.Marker(
    [max_price_row['latitude'], max_price_row['longitude']],
    popup=f"Max Price: {max_price}<br>Retailer: {max_price_row['retailer']}<br>Brand: {max_price_row['brand']}<br>Location: {max_price_row['address']}",
    tooltip="Max Price",
    icon=folium.Icon(color='red')
).add_to(min_max_map)

folium_static(min_max_map)

# Create a Folium map for all data points
st.header("Map of Fuel Prices")
map_center = [filtered_df['latitude'].mean(), filtered_df['longitude'].mean()] if not filtered_df.empty else [51.509865, -0.118092]
folium_map = folium.Map(location=map_center, zoom_start=6, tiles='CartoDB Positron')

# Add points to the map
for _, row in filtered_df.iterrows():
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=6,
        color=row['color'],
        fill=True,
        fill_color=row['color'],
        fill_opacity=0.7,
        popup=f"Retailer: {row['retailer']}<br>Brand: {row['brand']}<br>Price: {row[selected_fuel_type]}",
        tooltip=row['brand'],
    ).add_to(folium_map)

# Display the map
folium_static(folium_map)

# Historical Price Trends for Selected Retailer
st.header("Historical Price Trends for Selected Retailer")
historical_df_filtered = historical_df[(historical_df['retailer'] == selected_retailer)][['retailer', 'last_updated', selected_fuel_type]].dropna()
historical_df_filtered = historical_df_filtered.sort_values(by='last_updated', ascending=True)
historical_df_filtered['last_updated'] = historical_df_filtered['last_updated'].dt.strftime('%Y-%m-%d')

st.line_chart(historical_df_filtered, x='last_updated', y=selected_fuel_type)

# Historical Price Trends for All Retailers with Interactive Legend
st.header("Historical Price Trends for All Retailers")
historical_all_retailers = historical_df[['retailer', 'last_updated', selected_fuel_type]].dropna()

highlight = alt.selection_point(fields=['retailer'], on='mouseover', nearest=True)
base = alt.Chart(historical_all_retailers).encode(
    x='last_updated:T',
    y=alt.Y(selected_fuel_type, title='Price'),
    color='retailer:N'
)

lines = base.mark_line().encode(
    opacity=alt.condition(highlight, alt.value(1), alt.value(0.2))
).add_params(
    highlight
)

points = base.mark_circle().encode(
    opacity=alt.condition(highlight, alt.value(1), alt.value(0))
)

chart = lines + points
chart = chart.properties(
    width=800,
    height=400
).interactive()

st.altair_chart(chart, use_container_width=True)

# Additional Charts and Insights
st.header("Additional Insights")

# Retailer Comparison
st.write("### Retailer Comparison")
retailer_comparison = df.groupby('retailer')[selected_fuel_type].mean().sort_values()
st.bar_chart(retailer_comparison)

# Brand Comparison
st.write("### Brand Comparison")
brand_comparison = df.groupby('brand')[selected_fuel_type].mean().sort_values()
st.bar_chart(brand_comparison)

# Price Distribution by Fuel Type
st.write("### Price Distribution by Fuel Type")
fuel_distribution = df[fuel_types].melt(var_name='Fuel Type', value_name='Price')
fuel_distribution_chart = alt.Chart(fuel_distribution).mark_boxplot().encode(
    x='Fuel Type',
    y='Price'
).properties(
    width=600,
    height=400
)
st.altair_chart(fuel_distribution_chart, use_container_width=True)

# Top 5 Cheapest and Most Expensive Retailers
st.write("### Top 5 Cheapest and Most Expensive Retailers")
cheapest_retailers = retailer_comparison.head(5)
expensive_retailers = retailer_comparison.tail(5)

st.write("#### Top 5 Cheapest Retailers")
st.table(cheapest_retailers)

st.write("#### Top 5 Most Expensive Retailers")
st.table(expensive_retailers)
