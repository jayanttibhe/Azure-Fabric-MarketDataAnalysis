-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "8ff6cf9e-ee2c-4ff7-882e-2d4155df7602",
-- META       "default_lakehouse_name": "Market_Analysis",
-- META       "default_lakehouse_workspace_id": "994aed12-db33-4876-9c93-59284bfb860b",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "8ff6cf9e-ee2c-4ff7-882e-2d4155df7602"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "ed772ca8-ff63-4a6c-b920-4b58d8409204",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "ed772ca8-ff63-4a6c-b920-4b58d8409204",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- **1. Basic Technical Indicators Attributes**

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE OR REPLACE TABLE PriceData AS
-- MAGIC WITH PriceDataBase AS (
-- MAGIC     SELECT 
-- MAGIC         Ticker,
-- MAGIC         Date_Key,
-- MAGIC         Year,
-- MAGIC         CAST(`Open` AS DECIMAL(18,4)) AS `Open`,
-- MAGIC         CAST(`Close` AS DECIMAL(18,4)) AS `Close`,
-- MAGIC         CAST(`High` AS DECIMAL(18,4)) AS `High`,
-- MAGIC         CAST(`Low` AS DECIMAL(18,4)) AS `Low`,
-- MAGIC         CAST(Volume AS DECIMAL(18,4)) AS Volume,
-- MAGIC         CAST(Dividends AS DECIMAL(18,4)) AS Dividends,
-- MAGIC         CAST(Stock_Splits AS DECIMAL(18,4)) AS Stock_Splits,
-- MAGIC         AVG(CAST(`Close` AS DECIMAL(18,4))) OVER (
-- MAGIC             PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS `50_DMA`,
-- MAGIC         AVG(CAST(`Close` AS DECIMAL(18,4))) OVER (
-- MAGIC             PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS `200_DMA`,
-- MAGIC         AVG(CAST(`Close` AS DECIMAL(18,4))) OVER (
-- MAGIC             PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 209 PRECEDING AND CURRENT ROW) AS `10_Month_SMA`,
-- MAGIC         AVG(CAST(`Close` AS DECIMAL(18,4))) OVER (
-- MAGIC             PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS `52W_MA`
-- MAGIC     FROM stocks_history
-- MAGIC     --WHERE Ticker = 'SPY'
-- MAGIC ),
-- MAGIC 
-- MAGIC DailyChanges AS (
-- MAGIC     SELECT 
-- MAGIC         Ticker,
-- MAGIC         Date_Key,
-- MAGIC         `Close`,
-- MAGIC         CASE WHEN `Close` > LAG(`Close`, 1) OVER (PARTITION BY Ticker ORDER BY Date_Key) 
-- MAGIC              THEN `Close` - LAG(`Close`, 1) OVER (PARTITION BY Ticker ORDER BY Date_Key) ELSE 0 END AS Gain,
-- MAGIC         CASE WHEN `Close` < LAG(`Close`, 1) OVER (PARTITION BY Ticker ORDER BY Date_Key) 
-- MAGIC              THEN LAG(`Close`, 1) OVER (PARTITION BY Ticker ORDER BY Date_Key) - `Close` ELSE 0 END AS Loss
-- MAGIC     FROM PriceDataBase
-- MAGIC ),
-- MAGIC 
-- MAGIC RSI_Calculation AS (
-- MAGIC     SELECT 
-- MAGIC         Ticker, 
-- MAGIC         Date_Key, 
-- MAGIC         `Close`,
-- MAGIC         AVG(Gain) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS AvgGain,
-- MAGIC         AVG(Loss) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS AvgLoss
-- MAGIC     FROM DailyChanges
-- MAGIC ),
-- MAGIC 
-- MAGIC MACD_Calculation AS (
-- MAGIC     SELECT 
-- MAGIC         Ticker, 
-- MAGIC         Date_Key, 
-- MAGIC         `Close`,
-- MAGIC         AVG(`Close`) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS Short_EMA,
-- MAGIC         AVG(`Close`) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS Long_EMA
-- MAGIC     FROM PriceDataBase
-- MAGIC ),
-- MAGIC 
-- MAGIC Bollinger_Calculation AS (
-- MAGIC     SELECT 
-- MAGIC         Ticker, 
-- MAGIC         Date_Key, 
-- MAGIC         `Close`,
-- MAGIC         AVG(`Close`) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS SMA_20,
-- MAGIC         STDDEV(`Close`) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS StdDev_20
-- MAGIC     FROM PriceDataBase
-- MAGIC )
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC     pd.Ticker, 
-- MAGIC     pd.Date_Key, 
-- MAGIC     pd.Year, 
-- MAGIC     pd.`Open`,
-- MAGIC     pd.`Close`,
-- MAGIC     pd.`High`,
-- MAGIC     pd.`Low`,
-- MAGIC     pd.Volume,
-- MAGIC     pd.Dividends,
-- MAGIC     pd.Stock_Splits,
-- MAGIC     pd.`50_DMA`,
-- MAGIC     pd.`200_DMA`,
-- MAGIC     pd.`10_Month_SMA`,
-- MAGIC     pd.`52W_MA`,
-- MAGIC     --Daily Changes
-- MAGIC     dc.Gain,
-- MAGIC     dc.Loss,
-- MAGIC     --RSI,
-- MAGIC     rsi.AvgGain, 
-- MAGIC     rsi.AvgLoss,
-- MAGIC     --MACD
-- MAGIC     macd.`Short_EMA`, 
-- MAGIC     macd.`Long_EMA`,
-- MAGIC     --Bollinger
-- MAGIC     boll.SMA_20, 
-- MAGIC     boll.StdDev_20,
-- MAGIC     CAST(CASE WHEN rsi.AvgLoss = 0 THEN 100 ELSE 100 - (100 / (1 + (rsi.AvgGain / rsi.AvgLoss))) END AS DECIMAL(18,4)) AS RSI_14,
-- MAGIC     CAST((macd.Short_EMA - macd.Long_EMA) AS DECIMAL(18,4)) AS MACD,
-- MAGIC     CAST((boll.SMA_20 + (2 * boll.StdDev_20)) AS DECIMAL(18,4)) AS Bollinger_Upper,
-- MAGIC     CAST((boll.SMA_20 - (2 * boll.StdDev_20)) AS DECIMAL(18,4)) AS Bollinger_Lower
-- MAGIC FROM PriceDataBase pd
-- MAGIC LEFT JOIN DailyChanges dc ON pd.Ticker = dc.Ticker AND pd.Date_Key = dc.Date_Key
-- MAGIC LEFT JOIN RSI_Calculation rsi ON pd.Ticker = rsi.Ticker AND pd.Date_Key = rsi.Date_Key
-- MAGIC LEFT JOIN MACD_Calculation macd ON pd.Ticker = macd.Ticker AND pd.Date_Key = macd.Date_Key
-- MAGIC LEFT JOIN Bollinger_Calculation boll ON pd.Ticker = boll.Ticker AND pd.Date_Key = boll.Date_Key
-- MAGIC """)
-- MAGIC 
-- MAGIC # Verify the table was created
-- MAGIC df = spark.sql("SELECT * FROM PriceData LIMIT 10")
-- MAGIC display(df)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- **2. Basic Trading Model**

-- CELL ********************

CREATE OR REPLACE TABLE BasicTrading_1 AS
    SELECT 
        Ticker, 
        Date_Key, 
        Year, 
        Open,Close,High,Low,
        Volume,
        50_DMA,
        200_DMA,
        RSI_14,
        MACD,
        Bollinger_Upper,
        Bollinger_Lower,
        SMA_20,
        
        -- Trend Analysis
        CASE WHEN 50_DMA > 200_DMA THEN 'Bullish' ELSE 'Bearish' END AS MA_Trend,
        
        -- RSI Signals
        CASE 
            WHEN RSI_14 < 30 THEN 'Oversold'
            WHEN RSI_14 > 70 THEN 'Overbought'
            ELSE 'Neutral' 
        END AS RSI_Signal,
        
        -- Bollinger Band Signals
        CASE 
            WHEN Close < (SMA_20 - StdDev_20) THEN 'Lower Band (Oversold)'
            WHEN Close > (SMA_20 + StdDev_20) THEN 'Upper Band (Overbought)'
            ELSE 'Within Bands' 
        END AS Bollinger_Signal,
        
        -- MACD Signal
        CASE 
            WHEN MACD > 0 THEN 'Bullish'
            ELSE 'Bearish' 
        END AS MACD_Signal,
        
        -- Combined Trading Signal
        CASE
            WHEN RSI_14 < 30 
                 AND Close < (SMA_20 - (0.5 * StdDev_20))
                 AND 50_DMA > 200_DMA THEN 'Strong Buy'
            WHEN RSI_14 > 70 
                 AND Close > (SMA_20 + (0.5 * StdDev_20))
                 AND 50_DMA < 200_DMA THEN 'Strong Sell'
            WHEN RSI_14 < 35 
                 AND Close < SMA_20 
                 AND MACD > 0 THEN 'Buy'
            WHEN RSI_14 > 65 
                 AND Close > SMA_20 
                 AND MACD < 0 THEN 'Sell'
            ELSE 'Hold'
        END AS Trading_Action
    FROM PriceData


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

Select * from basictrading_1
WHERE Ticker = 'TQQQ'


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- **3. Advanced Trading Model**

-- CELL ********************

CREATE OR REPLACE TABLE AdvancedTradingModel AS
WITH 
-- Add volume-weighted indicators
VolumeAdjusted AS (
    SELECT *,
           AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) * 
           (1 + (Volume/AVG(Volume) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 20 PRECEDING AND CURRENT ROW))) AS VWAP_20,
           
           STDDEV(Close) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) * 
           (1 + (Volume/AVG(Volume) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 20 PRECEDING AND CURRENT ROW))) AS Volatility_Index
    FROM BasicTrading_1
),

-- Add momentum confirmation
MomentumConfirmation AS (
    SELECT *,
           Close - LAG(Close, 3) OVER (PARTITION BY Ticker ORDER BY Date_Key) AS Momentum_3D,
           Close - LAG(Close, 5) OVER (PARTITION BY Ticker ORDER BY Date_Key) AS Momentum_5D,
           (LAG(RSI_14, 1) OVER (PARTITION BY Ticker ORDER BY Date_Key) - RSI_14) AS RSI_Slope,
           (MACD - LAG(MACD, 1) OVER (PARTITION BY Ticker ORDER BY Date_Key)) AS MACD_Slope
    FROM VolumeAdjusted
),

-- Add institutional activity detection
InstitutionalActivity AS (
    SELECT *,
           CASE WHEN Volume > 2 * AVG(Volume) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 50 PRECEDING AND CURRENT ROW)
                     AND Close > Open 
                     AND Close > (High + Low)/2 THEN 'Institutional Buying'
                WHEN Volume > 2 * AVG(Volume) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 50 PRECEDING AND CURRENT ROW)
                     AND Close < Open 
                     AND Close < (High + Low)/2 THEN 'Institutional Selling'
                ELSE 'Normal Activity' END AS BigMoneySignal,
                
           CORR(Close, Volume) OVER (PARTITION BY Ticker ORDER BY Date_Key ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS PriceVolumeCorrelation
    FROM MomentumConfirmation
),

-- Calculate DMA differences first
DMADifferences AS (
    SELECT 
        Ticker,
        Date_Key,
        200_DMA,
        200_DMA - LAG(200_DMA, 5) OVER (PARTITION BY Ticker ORDER BY Date_Key) AS DMA_5DayDiff,
        200_DMA - LAG(200_DMA, 20) OVER (PARTITION BY Ticker ORDER BY Date_Key) AS DMA_20DayDiff
    FROM InstitutionalActivity
),

-- Add regime filter using pre-calculated differences
MarketRegime AS (
    SELECT 
        ia.*,
        AVG(dd.DMA_5DayDiff) OVER (PARTITION BY ia.Ticker ORDER BY ia.Date_Key ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS MA_TrendStrength,
        CASE 
            WHEN dd.DMA_20DayDiff > 0
                 AND (SELECT AVG(RSI_14) FROM BasicTrading_1 WHERE Ticker = ia.Ticker AND Date_Key BETWEEN DATEADD(day, -20, ia.Date_Key) AND ia.Date_Key) < 60
            THEN 'Bull Market'
            WHEN dd.DMA_20DayDiff < 0
                 AND (SELECT AVG(RSI_14) FROM BasicTrading_1 WHERE Ticker = ia.Ticker AND Date_Key BETWEEN DATEADD(day, -20, ia.Date_Key) AND ia.Date_Key) > 40
            THEN 'Bear Market'
            ELSE 'Neutral Market' 
        END AS MarketRegime
    FROM InstitutionalActivity ia
    JOIN DMADifferences dd ON ia.Ticker = dd.Ticker AND ia.Date_Key = dd.Date_Key
),

-- Corrected MultiTimeframe CTE using LEFT JOIN to avoid scalar subquery issues
MultiTimeframe AS (
    SELECT 
        mr.*,
        b_weekly.50_DMA AS Weekly_50DMA,
        b_weekly.RSI_14 AS Weekly_RSI
    FROM MarketRegime mr
    LEFT JOIN BasicTrading_1 b_weekly
        ON b_weekly.Ticker = mr.Ticker
        AND b_weekly.Date_Key = DATEADD(week, -1, mr.Date_Key)
)

SELECT 
    Ticker, Date_Key, Year, Close, Volume, 50_DMA, 200_DMA, RSI_14, MACD,
    Bollinger_Upper, Bollinger_Lower, SMA_20, MA_Trend, RSI_Signal, Bollinger_Signal, MACD_Signal,
    VWAP_20, Volatility_Index, Momentum_3D, Momentum_5D, RSI_Slope, MACD_Slope,
    BigMoneySignal, PriceVolumeCorrelation, MA_TrendStrength, MarketRegime, Weekly_50DMA, Weekly_RSI,
    
    -- Enhanced Trading Signals with 9-Filter Confirmation
    CASE
        -- Ultra High-Probability Buy (7+ confirmations)
        WHEN RSI_14 < 30 
             AND Close < (SMA_20 - (0.5 * (Bollinger_Upper - Bollinger_Lower)/2))
             AND 50_DMA > 200_DMA
             AND MA_TrendStrength > 0
             AND MarketRegime = 'Bull Market'
             AND BigMoneySignal = 'Institutional Buying'
             AND Momentum_3D > 0 AND Momentum_5D > 0
             AND RSI_Slope > 0
             AND MACD_Slope > 0
             AND Weekly_RSI < 40
             AND Weekly_50DMA < (SELECT AVG(50_DMA) FROM BasicTrading_1 
                                WHERE Ticker = m.Ticker
                                AND Date_Key BETWEEN DATEADD(month, -3, m.Date_Key) AND m.Date_Key)
             THEN 'Ultra High-Probability Buy'
             
        -- Ultra High-Probability Sell (7+ confirmations)
        WHEN RSI_14 > 70 
             AND Close > (SMA_20 + (0.5 * (Bollinger_Upper - Bollinger_Lower)/2))
             AND 50_DMA < 200_DMA
             AND MA_TrendStrength < 0
             AND MarketRegime = 'Bear Market'
             AND BigMoneySignal = 'Institutional Selling'
             AND Momentum_3D < 0 AND Momentum_5D < 0
             AND RSI_Slope < 0
             AND MACD_Slope < 0
             AND Weekly_RSI > 60
             AND Weekly_50DMA > (SELECT AVG(50_DMA) FROM BasicTrading_1 
                                WHERE Ticker = m.Ticker
                                AND Date_Key BETWEEN DATEADD(month, -3, m.Date_Key) AND m.Date_Key)
             THEN 'Ultra High-Probability Sell'
             
        -- High-Probability Buy (5+ confirmations)
        WHEN RSI_14 < 35 
             AND Close < SMA_20 
             AND MACD > 0
             AND (BigMoneySignal = 'Institutional Buying' OR MarketRegime = 'Bull Market')
             AND (Momentum_3D > 0 OR RSI_Slope > 0)
             AND Weekly_RSI < 50
             THEN 'High-Probability Buy'
             
        -- High-Probability Sell (5+ confirmations)
        WHEN RSI_14 > 65 
             AND Close > SMA_20 
             AND MACD < 0
             AND (BigMoneySignal = 'Institutional Selling' OR MarketRegime = 'Bear Market')
             AND (Momentum_3D < 0 OR RSI_Slope < 0)
             AND Weekly_RSI > 50
             THEN 'High-Probability Sell'
             
        ELSE 'Hold'
    END AS Trading_Action,
    
    -- Confidence Score (0-100%)
    CASE
        WHEN Trading_Action = 'Ultra High-Probability Buy' THEN 95
        WHEN Trading_Action = 'Ultra High-Probability Sell' THEN 95
        WHEN Trading_Action = 'High-Probability Buy' THEN 80
        WHEN Trading_Action = 'High-Probability Sell' THEN 80
        ELSE 0
    END AS Confidence_Score,
    
    -- Risk-Reward Ratio Estimate with corrected subqueries
    CASE
        WHEN Trading_Action LIKE '%Buy%' THEN 
            ((SELECT AVG(High - Close) FROM BasicTrading_1 
              WHERE Ticker = m.Ticker
              AND Date_Key BETWEEN DATEADD(day, -20, m.Date_Key) AND m.Date_Key) 
             / NULLIF((SELECT AVG(Close - Low) FROM BasicTrading_1 
                      WHERE Ticker = m.Ticker
                      AND Date_Key BETWEEN DATEADD(day, -20, m.Date_Key) AND m.Date_Key), 0))
        WHEN Trading_Action LIKE '%Sell%' THEN 
            ((SELECT AVG(Close - Low) FROM BasicTrading_1 
              WHERE Ticker = m.Ticker
              AND Date_Key BETWEEN DATEADD(day, -20, m.Date_Key) AND m.Date_Key) 
             / NULLIF((SELECT AVG(High - Close) FROM BasicTrading_1 
                      WHERE Ticker = m.Ticker
                      AND Date_Key BETWEEN DATEADD(day, -20, m.Date_Key) AND m.Date_Key), 0))
        ELSE NULL
    END AS Risk_Reward_Ratio
    
FROM MultiTimeframe m;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

Select * From AdvancedTradingModel
where 
Ticker = 'SPY'
 Order by Date_Key DESC   

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
