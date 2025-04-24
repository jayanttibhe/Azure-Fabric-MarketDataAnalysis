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

# **01.Calendar Table**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sequence, explode, expr, lit, date_format, weekofyear, year, quarter, dayofweek

# Initialize Spark session
spark = SparkSession.builder.appName("CalendarTable").getOrCreate()

# Define the date range
start_date = "1980-01-01"
end_date = "2030-12-31"

# Generate a sequence of dates
dates_df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as Date")

# Add additional columns to the calendar table
calendar_df = dates_df.withColumn("Year", year(col("Date"))) \
    .withColumn("Month", date_format(col("Date"), "MM")) \
    .withColumn("Day", date_format(col("Date"), "dd")) \
    .withColumn("DayName", date_format(col("Date"), "EEEE")) \
    .withColumn("WeekDay", dayofweek(col("Date"))) \
    .withColumn("DayOfYear", date_format(col("Date"), "D")) \
    .withColumn("WeekOfYear", weekofyear(col("Date"))) \
    .withColumn("Quarter", expr("concat('Q', quarter(Date))")) \
    .withColumn("IsWeekend", expr("dayofweek(Date) IN (1, 7)")) \
    .withColumn("IsHoliday", lit(False)) \
    .withColumn("QuarterYear", expr("concat('Q', quarter(Date), '-', year(Date))")) \
    .withColumn("MonthYear", date_format(col("Date"), "yyyyMM")) \
    .withColumn("WeekYear", expr("concat(year(Date), lpad(weekofyear(Date), 2, '0'))"))

# Show the generated calendar table
display(calendar_df)

# Save the calendar table as a Delta table
table_name = "Calendar_Table"  # Change this to your desired table name
if spark.catalog._jcatalog.tableExists(table_name):
    spark.sql(f"DROP TABLE {table_name}")
    print(f"Deleted existing table: {table_name}")

calendar_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
print(f"Saved new table: {table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **02.Rolling As Of Calendar**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sequence, explode, expr, lit, date_format, year, month

# Initialize Spark session
spark = SparkSession.builder.appName("YearMonthTable").getOrCreate()

# Define the date range
start_date = "1990-01-01"
end_date = "2030-12-31"

# Rolling window length in months
rolling_window_length = 36  

# Generate a sequence of dates
dates_df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 month)) as Date")

# Add additional columns to the YearMonth table
yearmonth_df = dates_df.withColumn("Year", year(col("Date"))) \
    .withColumn("Month", date_format(col("Date"), "MM")) \
    .withColumn("YearMonth", date_format(col("Date"), "yyyyMM"))

# Create a rolling "As Of Date" column
rolling_df = yearmonth_df.withColumn("AsOfDate", col("Date"))

# Generate the rolling calendar table with all combinations of YearMonth values up to the selected YearMonth
rolling_window_df = rolling_df.alias("base").crossJoin(rolling_df.alias("roll")) \
    .filter(expr(f"base.Date >= add_months(roll.Date, -{rolling_window_length - 1}) AND base.Date <= roll.Date")) \
    .select(
        col("roll.YearMonth").alias("RollingYearMonth"),
        col("base.YearMonth").alias("BaseYearMonth")
    )
# Filter the DataFrame to show only rows where the RollingYearMonth is greater than 202001
# filtered_df = rolling_window_df.filter(col("RollingYearMonth") < lit("202201"))

# Show the filtered DataFrame
display(rolling_window_df.count())
display(rolling_window_df)

# Save the rolling calendar table as a Delta table
table_name = "Rolling_YearMonth_Calendar_Table"  # Change this to your desired table name
if spark.catalog._jcatalog.tableExists(table_name):
    spark.sql(f"DROP TABLE {table_name}")
    print(f"Deleted existing table: {table_name}")

rolling_window_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
print(f"Saved new table: {table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM rolling_yearmonth_calendar_table \
Where RollingYearMonth = 202012")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
