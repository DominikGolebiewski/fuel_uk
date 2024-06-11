import duckdb
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import folium_static
import matplotlib.cm as cm
import matplotlib.colors as colors
import matplotlib.pyplot as plt
from neuralprophet import NeuralProphet

# Function to load data from DuckDB
def load_data():
    con = duckdb.connect('petro_dbt/petro.duckdb')
    query = """
        SELECT * 
        FROM petro
        
        INNER JOIN (SELECT retailer, MAX(last_updated) last_updated FROM petro GROUP BY 1) AS latest
            ON petro.retailer = latest.retailer AND petro.last_updated = latest.last_updated

        WHERE petro.retailer != 'Morrisons' AND address != 'GX11 1AA'
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
    
    df['color'] = df[fuel_type].apply(lambda x: colors.to_hex(colormap.to_rgba(x)))
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

# Function to forecast prices using NeuralProphet
def forecast_prices(historical_df, fuel_type):
    df_prophet = historical_df[['last_updated', fuel_type]].rename(columns={'last_updated': 'ds', fuel_type: 'y'})
    model = NeuralProphet()
    model.fit(df_prophet, freq='D')
    future = model.make_future_dataframe(df_prophet, periods=30)
    forecast = model.predict(future)
    return forecast

# Load the data
df = load_data()
historical_df = load_historical_data()

# Extract unique fuel types and retailers
fuel_types = ['b7', 'e10', 'e5', 'sdv']
retailers = df['retailer'].unique()

# Check and convert data types
df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
historical_df['last_updated'] = pd.to_datetime(historical_df['last_updated'])

# Streamlit sidebar for user selection
selected_fuel_type = st.sidebar.selectbox('Select Fuel Type', fuel_types)

# Filter dataframe based on the selected fuel type and retailer
filtered_df = df[(~df[selected_fuel_type].isnull())]

# Assign colors to prices
filtered_df = assign_colors(filtered_df, selected_fuel_type)

# Overall Statistics
st.title("Fuel Price Analysis and Insights in the UK")
st.write("Welcome to our comprehensive analysis of fuel prices across various retailers in the UK. This report aims to provide detailed insights into fuel pricing patterns, trends, and future forecasts. We will explore the data through various lenses, including minimum and maximum prices, average prices, regional analysis, and historical trends.")
min_price = df[selected_fuel_type].min()
max_price = df[selected_fuel_type].max()
mean_price = df[selected_fuel_type].mean()

min_price_row = df[df[selected_fuel_type] == min_price].iloc[0]
max_price_row = df[df[selected_fuel_type] == max_price].iloc[0]

st.header("Breakdown of Fuel Prices")
st.write("### Overall Statistics")
st.write(f"**Minimum Price:** {min_price} (Retailer: {min_price_row['retailer']}, Brand: {min_price_row['brand']}, Location: {min_price_row['address']})")
st.write(f"**Maximum Price:** {max_price} (Retailer: {max_price_row['retailer']}, Brand: {max_price_row['brand']}, Location: {max_price_row['address']})")
st.write(f"**Mean Price:** {mean_price}")

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



# Retailer-Specific Statistics
# st.header("Retailer-Specific Statistics")
# for retailer in retailers:
#     retailer_data = df[df['retailer'] == retailer]
#     min_price = retailer_data[selected_fuel_type].min()
#     max_price = retailer_data[selected_fuel_type].max()
#     mean_price = retailer_data[selected_fuel_type].mean()
#     st.write(f"**{retailer}**")
#     st.write(f"Min: {min_price}, Max: {max_price}, Mean: {mean_price}")

# Regional Analysis (NUTS)
# st.header("Regional Analysis")
# # Assuming 'region' column exists in the data
# regional_data = df.groupby('region')[selected_fuel_type].mean().sort_values()
# st.write("Best regions to buy fuel based on average price:")
# st.write(regional_data.head())

# Create a Folium map
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

# Historical Price Trends
st.header("Historical Price Trends")
historical_df_filtered = historical_df[['retailer', 'last_updated', selected_fuel_type]].dropna()
historical_df_filtered = historical_df_filtered.sort_values(by='last_updated', ascending=True)
historical_df_filtered['last_updated'] = historical_df_filtered['last_updated'].dt.strftime('%Y-%m-%d')

fig, ax = plt.subplots(figsize=(12, 8))
colors = plt.cm.tab20.colors  # Use a colormap with enough colors

st.bar_chart(historical_df_filtered, x='last_updated', y=selected_fuel_type, color='retailer')

for i, retailer in enumerate(retailers):
    retailer_data = historical_df_filtered[historical_df_filtered['retailer'] == retailer]
    ax.plot(retailer_data['last_updated'], retailer_data[selected_fuel_type], label=f"{retailer}", color=colors[i % len(colors)], marker='o')



ax.set_title(f'{selected_fuel_type.upper()} Prices Over Time for All Retailers')
ax.set_xlabel('Date')
ax.set_ylabel('Price')
ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.xticks(rotation=45)
plt.grid(True)
st.pyplot(fig)

# # Price Forecast
# st.header("Price Forecast")
# forecast = forecast_prices(historical_df, selected_fuel_type)
# fig2, ax2 = plt.subplots(figsize=(12, 8))
# ax2.plot(forecast['ds'], forecast['yhat1'], label='Forecast', color='blue')
# ax2.fill_between(forecast['ds'], forecast['yhat1_lower'], forecast['yhat1_upper'], color='blue', alpha=0.2)
# ax2.set_title(f'Forecast of {selected_fuel_type.upper()} Prices')
# ax2.set_xlabel('Date')
# ax2.set_ylabel('Price')
# plt.xticks(rotation=45)
# plt.grid(True)
# st.pyplot(fig2)

# Running the Streamlit App
if __name__ == '__main__':
    st.title('Fuel Prices Visualization')
    st.write('Select a fuel type from the sidebar to visualize the locations of fuel stations by retailer and brand.')
    st.write('The line chart below shows the historical prices over time for all retailers.')
    st.write('The forecast chart predicts the future trends of fuel prices.')
