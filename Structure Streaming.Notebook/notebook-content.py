# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8ff6cf9e-ee2c-4ff7-882e-2d4155df7602",
# META       "default_lakehouse_name": "Market_Analysis",
# META       "default_lakehouse_workspace_id": "994aed12-db33-4876-9c93-59284bfb860b",
# META       "known_lakehouses": [
# META         {
# META           "id": "8ff6cf9e-ee2c-4ff7-882e-2d4155df7602"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

pip install pyspark yfinance pandas pandas_datareader

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import from_json, col, lit,current_timestamp
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
        price = ticker.history(period="max")["Close"].iloc[-1]
        timestamp = ticker.history(period="max").index[-1]
        data.append((symbol, price, timestamp))
    return data

# Create a streaming DataFrame
def generate_streaming_data(spark):
    symbols = ["NVDA"]  # Add more symbols as needed
    return spark.createDataFrame(fetch_stock_data(symbols), schema=schema)

    # Set up the streaming query
    stream_df = spark.readStream.format("rate").load()
    # stream_df = stream_df.withColumn("value", generate_streaming_data(spark))
    stream_df.withColumn("symbol", lit(symbols)) \
                        .withColumn("price", lit(0.0).cast(DoubleType())) \
                        .withColumn("timestamp", current_timestamp())
    display(stream_df)

# query = (stream_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "/Files/Market/Streaming/checkpoint")
#     .trigger(once=True)
#     .start("/Files/Market/"))

# # Wait for the streaming query to terminate
# # query.awaitTermination()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Market_Analysis.stocks_history LIMIT 1000")
display(df)

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
