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

# MARKDOWN ********************

# **1.0 Read all requried tickers and save those in Bronze layer**

# CELL ********************

import yfinance as yf
import pandas as pd
# Define an array of tickers
tickers = ['SPY', 'QQQ', 'TQQQ', 'UPRO', 'TMF']

# Loop through each ticker
for ticker in tickers:
    try:
        # Fetch historical data for the ticker
        data = yf.Ticker(ticker)
        history_data = data.history(period="max")

        # Save to CSV in Azure Fabric Lakehouse
        csv_path = f"abfss://Market_Data_Analysis@onelake.dfs.fabric.microsoft.com/Market_Analysis.Lakehouse/Files/Market/Bronze/{ticker}.csv"
        history_data.to_csv(csv_path)
    
        print(f"Data for {ticker} successfully exported to {csv_path}")
    
    except Exception as e:
        
        print(f"Error processing {ticker}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **2.0 Read all necessary Macroeconomic Data and  save those in Bronze layer**

# CELL ********************

import pandas as pd
import pandas_datareader.data as web
import datetime

# Define time range
start_date = datetime.datetime(1990, 1, 1)
end_date = datetime.datetime.now()

# Collect Federal Funds Rate (FRED)
ff_rate = web.DataReader('FEDFUNDS', 'fred', start_date, end_date)

# Collect 10-Year Treasury Yield (FRED)
treasury_yield = web.DataReader('DGS10', 'fred', start_date, end_date)

# Collect 20+ Year Treasury Yield (FRED series ID: DGS20)
long_term_treasury_yield = web.DataReader('DGS20', 'fred', start_date, end_date)

# Collect Oil Prices (WTI Crude) from FRED
oil_prices = web.DataReader('DCOILWTICO', 'fred', start_date, end_date)

# Fetch VIX data from FRED
vix_data = web.DataReader('VIXCLS', 'fred', start_date, end_date)

# Combine data into a single DataFrame
data = pd.concat([ff_rate, treasury_yield, long_term_treasury_yield, oil_prices, vix_data], axis=1)
data.columns = ['Federal Funds Rate', '10-Year Treasury Yield', '20-Year Treasury Yield', 'Oil Prices', 'VIX']

# Save to CSV in Azure Fabric Lakehouse
csv_path = 'abfss://Market_Data_Analysis@onelake.dfs.fabric.microsoft.com/Market_Analysis.Lakehouse/Files/Market/Bronze/Macroeconomic_Data.csv'
data.to_csv(csv_path)

# print(data.head)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Market/Bronze/SPY.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.read.format("csv").option("header","true").load("Files/Market/Bronze/Macroeconomic_Data.csv")
# display(df.limit(200))
# Sort the DataFrame by 'Date' in descending order and limit to 200 rows
df_sorted = df.orderBy(F.col("Date").desc()).limit(200)

# Show the result
display(df_sorted.limit(200))

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

# CELL ********************

# MAGIC %%sql
# MAGIC     SELECT  * FROM 
# MAGIC     delta.`abfss://Market_Data_Analysis@onelake.dfs.fabric.microsoft.com/Market_Analysis.Lakehouse/Files/Market/Silver/QQQ_delta`
# MAGIC     --delta.`Files/Market/Silver/QQQ_delta`
# MAGIC     --where Open = 4.577485250209593
# MAGIC     --WHERE Year >= 2023
# MAGIC     Order bY Date desc
# MAGIC Limit 100

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
