"""
Pyspark ETL pipeline for coinGecko Cryptocurrency Data 

Purpose :- 
Extract cryptocurrency data from csv file , analyze it , clean invalid records , 
apply business logic , calculate market statistics , validate data quality and save 
the final result. 

ETL :- EXTRACT TRANSFORM LOAD
"""

from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from pyspark.sql.types import *

from datetime import datetime

# ================================================
#  STEP 1 : INITILIZE SPARK SESSION
# ================================================

print("PYSPARK ETL PIPELINE FOR COINGECKO DATA")

print("\n [STEP 1] Initializing Spark session....")

# Adaptive query Execution helps spark optimized query execution at runtime.
# Partitions coalescing can combine small partitions to reduce overhead

spark = SparkSession.builder \
         .appName("coinGeckoETL") \
         .config("spark.sql.adaptive.enabled","true") \
         .config("spark.sql.adaptive.coalescePartitions.enabled","true") \
         .getOrCreate()

print("Spark Session Created Successfully")
print(f"Spark Version : {spark.version}")

# ================================================
#  STEP 2 : EXTRACTION - LOAD RAW DATA 
# ================================================

print("\n [STEP 2 ] EXTRACTION - Loading data from csv...")

df=spark.read.csv("input/coingecko_raw_data.csv",
                  header=True,
                  inferSchema=True)

print("Data Loaded Successfully")
print(f"Schema : {df.schema}")
print(f"Total Records : {df.count()}")

# Cache the Dataframe because it will be reused in multiple operations 
# Cached data may reduce repeated read and computations
df.cache()

# ================================================
#  STEP 3 : EXPLORATORY DATA ANALYSIS [EDA] 
# ================================================

print("\n [STEP 3] EXPLORATORY DATA ANALYSIS....")

# count rows and columns 
print(f"Data Shape : {df.count()} rows , {len(df.columns)} Columns")

# Display the Schema - columnname and DataType
df.printSchema()

# Calculate descriptiove Statistics for supported columns 
# Example include count , mean , standard deviation , minimum and maximum
print("\n Basic Statistics:")
df.describe().show()

# Count null values seperately for each column
print("\n Missing value per column :")
df.select([F.count(F.when(F.col(c).isNull(), F.col(c))
        ).alias(c)
        for c in df.columns
]).show()


# ================================================
#  STEP 4 : DATA CLEANING  
# ================================================

print("\n [STEP 4]  DATA CLEANING....")

# -------------------------------------------
#  STEP 4.1 : HANDLE MISSING VALUES
# -------------------------------------------

print("Handling Null Values....")

df_clean = df.na.fill({
    "current_price" : 0,
    "market_cap":0,
    "total_volume" : 0,
    "circulating_supply":0
})

# if max_supply is null , use circulating_supply as a fallback 
# F.coalesce returns the first not null value 
df_clean=df_clean.withColumn(
     "max_supply",
     F.coalesce(
         F.col("max_supply"),
         F.col("circulating_supply")
     )
)

# -------------------------------------------
#  STEP 4.2 : REMOVE DUPLICATE RECORDS
# -------------------------------------------

print("Removing duplicate records...")

# count records before duplicate removal 
initial_count = df_clean.count()

# Remove rows that have identical values across all columns 
df_clean= df_clean.dropDuplicates()

# count records After duplicate removal 
final_count = df_clean.count()

#Calculate how many duplicatre rows were removed 
print(f"Duplicate removed : {initial_count - final_count}")



# -------------------------------------------
#  STEP 4.3 : REMOVE INVALID PRICE
# -------------------------------------------

print("Removing records with invalid prices...")

# count records before filtering 
initial_count = df_clean.count()

# keep only records whose current price is greater than zero 
# This removes zero and negative prices
df_clean = df_clean.filter(F.col("current_price") > 0)

# Count records After filtering 
final_count = df_clean.count()

#Calculate 
print(f"Invalid price records removed : {initial_count - final_count}")

# Display the total number of records remaining after cleaning 
print(f"Records After cleaning : {df_clean.count()}")


# =====================================================
#  STEP 5: DATA TRANSFORMATION - Apply business logic 
# =====================================================


print("\n [STEP 5] DATA TRANSFORMATION......")

# -------------------------------------------
# 5.1 CREATE DERIVED COLUMNS
#-------------------------------------------

print("Creating Derived Columns.....")

# Calculate the ratio of current price to marke capitalization
# Adding 1 to the denominator avoids division by zero 

df_clean = df_clean.withColumn(
    "price_to_market_cap_ratio",
    F.col("current_price") / (F.col("market_cap")+1)
)


# Calcuate the ratio of trading volume to market capitilization 
df_clean = df_clean.withColumn(
    "volume_to_market_cap_ratio",
    F.col("total_volume") / (F.col("market_cap")+1)
)


# -----------------------------------------------
# 5.2 CATEGORIZE CRYPTOCURRENCIES BY MARKET CAP
#-----------------------------------------------

print("Categorizing coins by market cap......")

# F.when works like if/elif in python 
# F.otherwise specifies the default category 

df_clean = df_clean.withColumn(
    "market_cap_category",
    F.when(
        F.col("market_cap") >= 100000000000 , "Large Cap"
    ).when(
        F.col("market_cap") >= 10000000000 , "Mid Cap"
    ).when(
        F.col("market_cap") >= 1000000000 , "Small Cap"
    ).otherwise("Micro Cap")
)


# -------------------------------------------
# 5.3 CATEGORIZE 24-HOUR PRICE CHANGES
#------------------------------------------

print("Categorizing Price changes....")

# Classify coins according to 24 hours price change percentage

df_clean = df_clean.withColumn(
    "price_change_category",
    F.when(
        F.col("price_change_percentage_24h") >= 5,
        "Strong Positive"
    ).when(
        F.col("price_change_percentage_24h") >= 0,
        "Positive"
    ).when(
        F.col("price_change_percentage_24h") >= -5,
        "Negative"
    ).otherwise("Strong Negative")
)


# -------------------------------------------
# 5.4  EXTRACT DATE COMPONENTS
#------------------------------------------

# ATH means All Time High

# Extract the day
df_clean = df_clean.withColumn(
    "ath_day",
    F.day("ath_date")
)

# Extract the Year
df_clean = df_clean.withColumn(
    "ath_day",
    F.year("ath_date")
)

# Extract the month
df_clean = df_clean.withColumn(
    "ath_day",
    F.month("ath_date")
)

print("Data Transformation Completed Sucessfully")


# =====================================================
#  STEP 6: DATA AGGREGATION 
# =====================================================

print("\n [STEP 6] DATA AGGREGATION...")

# --------------------------------------------------
# 6.1   CALCULATE STATISTICS BT MARKET CAP CATEGORY 
#---------------------------------------------------

print("Aggregating data by market cap  category")

# groupBy create group of cryptocurrencies with the same category
# agg calculate summary statistics for each group

category_stats = df_clean.groupBy(
    "market_cap_category"
).agg(
    # Average cryptocurrency price 
    F.mean("current_price").alias("avg_price"),

    F.median("current_price").alias("median_price"),

    F.stddev("current_price").alias("std_price"),

    F.sum("market_cap").alias("total_market_cap"),

    F.count("*").alias("coin_count"),

    F.sum("total_volume").alias("total_volume")
)

# Display aggreated statistics
category_stats.show()


# --------------------------------------------------
# 6.2 FIND THE TOP 10 COINS BY MARKET CAPITILIZATION
#---------------------------------------------------

print("\n Finding top 10 cryptocurrencies by market cap...")

# sort market capitiliation in descending order

top_10 = df_clean.orderBy(
    F.desc("market_cap")
).limit(10)

top_10.select(
    "name",
    "symbol",
    "current_price",
    "market_cap"
).show()

# ===============================================================
# STEP 7: DATA VALIDATION
# ===============================================================

print("\n [STEP 7] DATA VALIDATION.....")

# --------------------------------------------------
# 7.1 CHECK FOR NEGATIVE PRICES
#---------------------------------------------------

# Count rows where the current price is below zero 

negative_prices = df_clean.filter(
    F.col("current_price") < 0
).count()

# print warning if any negative prices are found
if negative_prices > 0:
    print(f"warning : Found {negative_prices} records"
         "with negative prices")
else:
    print("No negative prices found")

# --------------------------------------------------
# 7.2 CHECK FOR ZERO MARKET CAP WITH A POSITIVE CURRENT PRICE 
#---------------------------------------------------

zero_mcap=df_clean.filter(
    (F.col("market_cap") == 0) &
    (F.col("current_price") > 0)
).count()

# Records
if zero_mcap > 0:
     print(f"warning : Found {zero_mcap} records with zero market cap"
             "but positive price")
else:
    print("No zero-market-cap found")


# ===============================================================
# STEP 8: LOADING - SAVE PROCESSED DATA
# ===============================================================

print("\n [STEP 8] LOADING - saving cleaned data ")

# --------------------------------------------------
# 8.1  SAVE THE CLEANED DATA
#---------------------------------------------------

output_path = "output/coingecko_cleaned_data"

df_clean.write.mode("overwrite").csv(
    output_path,
    header=True
)
print(f"Cleaned Data saved to : {output_path}")

# --------------------------------------------------
# 8.2  SAVE THE AGGREGATED STATISTICS
#---------------------------------------------------

agg_output_path = "output/coingecko_aggregated_data"

category_stats.write.mode("overwrite").csv(
    agg_output_path,
    header=True
)

print(f"Aggregated Data saved to : {agg_output_path}")

# --------------------------------------------------
# 8.3 SAVE THE TOP 10 CRYPTOCURRENICES
#---------------------------------------------------

top10_output_path = "output/coingecko_top10_coins_data"

top_10.select(
    "name",
    "symbol",
    "current_price",
    "market_cap"
).write.mode("overwrite").csv(
    top10_output_path,
    header=True
)
print(f"Top 10 coins saved to : {top10_output_path}")


# ===========================================
# REPORT/SUMMARY
# ===========================================

print("PYSPARK ETL PIPELINE SUMMARY")

print(f"Original Records : {df.count()}")
print(f"Cleaned Records : {df_clean.count()}")

# Calculate the removed records
print(f"Records Removed : {df.count() - df_clean.count()}")

print("Output Datasets : Cleaned data , category statistics , top 10 coins")

# ==============================================

spark.stop()
print("Spark session stopped successfully")