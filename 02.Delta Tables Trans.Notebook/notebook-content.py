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
# META       "default_lakehouse_workspace_id": "994aed12-db33-4876-9c93-59284bfb860b"
# META     }
# META   }
# META }

# MARKDOWN ********************

# **02.Transfornation to Silver layer**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, current_timestamp,lit,to_date

# Initialize Spark session
spark = SparkSession.builder.appName("ProcessTickers").getOrCreate()

abss_path = "abfss://Market_Data_Analysis@onelake.dfs.fabric.microsoft.com/Market_Analysis.Lakehouse/Files"

# Define the path to the CSV files in the Data Lake
base_path = f"{abss_path}/Market/Bronze/"

# List of tickers (you can also dynamically list files in the directory)
tickers = ['SPY', 'QQQ', 'TQQQ', 'UPRO', 'TMF']

# Loop through each ticker
for ticker in tickers:
    try:
        # Define the path to the CSV file for the current ticker
        csv_path = f"{base_path}{ticker}.csv"

        # Read the CSV file into a DataFrame
        df = spark.read.csv(csv_path, header=True, inferSchema=True)

        # Replace spaces in column names with underscores
        df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])

        # Add new columns
        df = df.withColumn("Date_Key", to_date(col("Date")))            # Convert timestamp to date
        df = df.withColumn("Year", year(col("Date")))                   # Extract year from the 'Date' column
        df = df.withColumn("ingestion_time", current_timestamp())       # Add current timestamp
        df = df.withColumn("Ticker", lit(ticker)) 

        # Get Schema (Column Names)
        columns = df.columns
        print("Columns in Table:", columns)

        # Rearrange columns
        df = df.selectExpr("Ticker", "Date", "Date_Key", "Year", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits", "Capital_Gains", "ingestion_time")

        # Define the output path for Delta format
        delta_path = f"{abss_path}/Market/Silver/{ticker}_delta"

        # Save the DataFrame in Delta format with partitioning by 'Year'
        df.write.format("delta") \
               .partitionBy("Year") \
               .mode("overwrite") \
               .option("mergeSchema", "true")\
               .save(delta_path)

        print(f"Processed and saved {ticker} data to Delta format at {delta_path}")
  
    except Exception as e:
        print(f"Error processing {ticker}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC     SELECT * FROM delta.`abfss://Market_Data_Analysis@onelake.dfs.fabric.microsoft.com/Market_Analysis.Lakehouse/Files/Market/Silver/UPRO_delta`
# MAGIC    -- where Open = 4.577484299265039
# MAGIC     WHERE Year >= 2023
# MAGIC     Limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
