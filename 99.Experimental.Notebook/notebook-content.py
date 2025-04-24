# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0add07fd-9b03-4609-9b2e-4a72fa2da714",
# META       "default_lakehouse_name": "LakehouseTraining",
# META       "default_lakehouse_workspace_id": "fe776b56-6de2-4daf-8e09-b5b2494d3cf7"
# META     }
# META   }
# META }

# MARKDOWN ********************

# Incremental Code

# MARKDOWN ********************

# **Apache Stream Example**

# CELL ********************

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import yfinance as yf

# Define the schema for the incoming data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Function to fetch real-time stock data
def fetch_stock_data(symbols):
    data = []
    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        price = ticker.history(period="1m")["Close"].iloc[-1]
        timestamp = ticker.history(period="1m").index[-1]
        data.append((symbol, price, timestamp))
    return data

# Create a streaming DataFrame
def generate_streaming_data(spark):
    symbols = ["AAPL", "GOOGL", "MSFT"]  # Add more symbols as needed
    return spark.createDataFrame(fetch_stock_data(symbols), schema=schema)

# Set up the streaming query
stream_df = spark.readStream.format("rate").load()
stream_df = stream_df.withColumn("value", generate_streaming_data(spark))

# Write the streaming data to a Delta table
query = (stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoint")
    .trigger(processingTime="10 seconds")
    .start("/delta/stock_prices"))

# Wait for the streaming query to terminate
query.awaitTermination()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(filename='data_update.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Define an array of tickers
tickers = ['SPY', 'QQQ', 'TQQQ', 'UPRO', 'TMF']

# Loop through each ticker
for ticker in tickers:
    try:
        # Define the path to the existing CSV file
        csv_path = f"abfss://Market_Data_Analysis@onelake.dfs.fabric.microsoft.com/Market_Analysis.Lakehouse/Files/Market/Bronze/{ticker}.csv"

        # Check if the file already exists
        try:
            # Load existing data
            existing_data = pd.read_csv(csv_path, index_col='Date', parse_dates=True)
            last_date = existing_data.index[-1]  # Get the last date in the existing data
            start_date = last_date + timedelta(days=1)  # Start fetching data from the next day
            logging.info(f"Existing data found for {ticker}. Last date: {last_date}")
        except FileNotFoundError:
            # If the file doesn't exist, fetch all historical data
            existing_data = pd.DataFrame()
            start_date = "1900-01-01"  # Fetch all available data
            logging.info(f"No existing data found for {ticker}. Fetching all historical data.")

        # Fetch new data from the last available date to today
        new_data = yf.download(ticker, start=start_date, end=datetime.now())

        # If new data is available, append it to the existing data
        if not new_data.empty:
            updated_data = pd.concat([existing_data, new_data])
            
            # Remove duplicates (if any)
            updated_data = updated_data[~updated_data.index.duplicated(keep='last')]

            # Save to CSV, ensuring headers are written only once
            updated_data.to_csv(csv_path, mode='w', index=True)  # Overwrite the file with updated data
            logging.info(f"Updated data for {ticker} successfully exported to {csv_path}")
            print(f"Updated data for {ticker} successfully exported to {csv_path}")
        else:
            logging.info(f"No new data available for {ticker}")
            print(f"No new data available for {ticker}")

    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")
        print(f"Error processing {ticker}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install pandas_datareader

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
