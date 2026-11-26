"""
 PYSPARK ETL pipeline for CoiGecko Cryptocurrency Data 

 Purpose :- 
 Extract cryptocurrency data from CSV , analyze it , clean invalid records, apply business logic
 , calcalate market statistic , validate data equality and save the final results

 ETL = Extract + Transform + Load
"""

# Import sparksession to create and manage a spark application
from pyspark.sql import SparkSession

# Import Spark SQL functions sucha col , when , sum , mean  and count
from pyspark.sql import functions as F

# Import spark data types for defining schema when needed
from pyspark.sql.types import *

# Import datetime for data and time operations
# from datetime import datetime

# ==========================================================
#    STEP : 1 Initilize spark session
# =========================================================

print("PYSPARK ETL PIPELINE FOR COINGECKO DATA ")

print("\n [STEP 1] Initlizing Spark session .....")

# create a spark application named CoinGeckoETL

# Adaptive Query Exection helps spark optimize query execution at run time 
# Partition coalescing can combine small partitiona to reduce overhead
spark = SparkSession.builder.appName("CoinGeckoETL") \
         .config("spark.sql.adaptive.enabled","true") \
          .config("spark.sql.coalescePartitions.enabled","true") \
          .getOrCreate()

print("Spark session created sucessfully...")
print(f"Spark version : {spark.version}")

# ==========================================================
#    STEP : 2  EXTRACTION - LOAD RAW DATA
# ==========================================================

print("\n [STEP 2] EXTRACTION - Loading data from csv...")

df = spark.read.csv(
    "input/coingecko_raw_data.csv",
    header=True,
    inferSchema=True
)

# Display basic information about the extract data 
print("Data Loaded Sucessfully")
print(f"Schema : {df.schema}")
print(f"Total Records : {df.count()}")

# cache the Dataframe because it will be reused in multiple operation 
# cached data may reduce read and recomputation
df.cache()
# ==========================================================
#    STEP : 3 EDA (EXPLORATORY DATA ANAYLSIS)
# ==========================================================

print("\n [STEP 3 ] EXPLORATORY DATA ANAYLSIS ...")

# count rows and indentify the number of column 
print(f"Data Shape : {df.count()} rows , {len(df.columns)} Columns")

# Display column name and their spark data types
df.printSchema()

# Calculate description Statistics for supprted column 
# Example include count , mean , standard deviation ,minimum and maximum 
print("\n Basic Statisctics :")
df.describe().show()

# Count null values seperatly for every column
print("\n Missing values per Column :")

df.select([
    F.count(
        F.when(F.col(c).isNull(),F.col(c))
        ).alias(c)
        for c in df.columns
]).show()


# ==========================================================
#   STEP 4 DATA CLEANING
# ==========================================================

print("\n [STEP 4] DATA CLENING ...")

# ==============================
# 4.1 HANDLE MISSING VALUES
# ==============================

print("Handling missing values...")

# Replace NULL values in selected numeric column with zero 
df_clean = df.na.fill({
    "current_price":0,
    "market_cap":0,
    "total_volume":0,
    "circulating_supply":0
})

# If max_supply is NULL , use circulating_supply as a fallback 

df_clean = df_clean.withColumn(
    "max_supply",
    F.coalesce(
        F.col("max_supply"),
        F.col("circulating_supply")
    ))

# ==================================
#  4.2  REMOVE DUPLICATE RECORDS
# =================================

print("Removing duplicate Records.....")

# count records before duplicate removal
initial_count = df_clean.count()

# Remove rows that have identical values across all columns
df_clean = df_clean.dropDuplicates()

# Count records after duplicate removal 
final_count = df_clean.count()

print(f"Duplicate removed : {initial_count - final_count}" )

# ==================================
#  4.2  REMOVE INVALID PRICE 
# =================================

print("Removing records with invalid price...")

# count records before filtering
initial_count = df_clean.count() 

# keep only records whose current price is greater than zero 
df_clean = df_clean.filter(F.col("current_price") > 0)

# count records after filtering 
final_count = df_clean.count()

print(f"Invalid price records removed : {initial_count - final_count}" )

# Display the total number of records remaining after cleaning 
print(f"Records after cleaning : {df_clean.count()}")

# ==================================================
# STEP -5 : DATA TRANSFORMATION - APPLY BUSINESS LOGIC  
# ==================================================

print("\n [STEP 5] DATA TRANSFORMATION...")

# ==================================================
# STEP -5.1 : CREATE DERIVED COLUMNS
# ==================================================

# Calcuate the ratio of current price to maket capitalization 
# adding 1 to the denomination avoids division by zero 

df_clean = df_clean.withColumn(
    "price_to_market_cap_ratio",
    F.col("current_price") / (F.col("market_cap")+1)
)

# Calculate trading volume reltive to market capitalization 
# This ratio can help compare trading activity across cryptocurrencies
df_clean = df_clean.withColumn(
    "volumne_to_market_cap_ratio",
    F.col("total_volume") / (F.col("market_cap")+1)
)

# ==================================================
# STEP 5.2 CATEGORIZE CRYPTOCURRENCIES BY MARKET CAP
# ==================================================

print("categorizing coins by market cap...")

df_clean = df_clean.withColumn(
    "market_cap_category",
    F.when(
        F.col("market_cap") >= 100000000000 , "Large cap"
    ).when(
        F.col("market_cap") >= 10000000000 , "Mid cap"
    ).when(
        F.col("market_cap") >= 1000000000 , "Small cap"
    ).otherwise("Micro Cap")
)

# ==========================================
# STEP 5.3 CATEGORIZE 24-HOURS PRICE CHANGES
# ==========================================

print("Categorizing Price Changes .....")

# classify coins according to their 24-Hours price change percentage 

df_clean = df_clean.withColumn(
    "price_change_category",
    F.when(
        F.col("price_change_percentage_24h") >=5,
        "Strong positive"
    ).when(
        F.col("price_change_percentage_24h") >=0,
        "Positive"
    ).when(
        F.col("price_change_percentage_24h") >= -5,
        "Negative"
    ).otherwise("Strong Negative")
)

# ==========================================
# STEP 5.4 EXTRACT DATA COMPONENT
# ==========================================

# Convert ath_date into spark timestamp
# ATH mean All Time High

df_clean = df_clean.withColumn(
    "ath_day",
    F.day("ath_date")
)

#Extract the year from the all time high date 
df_clean = df_clean.withColumn(
    "ath_year",
    F.year("ath_date")
)


#Extract the month from the all time high date 
df_clean = df_clean.withColumn(
    "ath_month",
    F.month("ath_date")
)

print("Data transformation completed sucessfully")

# ==============================================
# STEP 6 DATA AGGREGATION
#===============================================

print("\n [STEP 6] DATA AGGREGATION.....")

#------------------------------------------------
# 6.1 CALCULATE STATISTICS BY MARKET CAP CATEGORY
#-----------------------------------------------
print("Aggregating data by maket cap category...")


# group by create group of cryptocurrencies with the same category
# agg calcuates summary statistics for each group

category_stats = df_clean.groupBy(
    "market_cap_category").agg(
        F.mean("current_price").alias("avg_price"),
        F.median("current_price").alias("median_price"),
        F.stddev("market_cap").alias("std_price"),
        F.sum("market_cap").alias("total_market_cap"),
        F.count("*").alias("coin_count"),
        F.sum("total_volume").alias("total_volume")
    )

print("\n category statistics:")
category_stats.show()


#------------------------------------------------
# 6.2 FIND THE TOP 10 COINS BY MARKET CAPITALIZATION
#-----------------------------------------------

print("\n Finding top 10 cryptocurrences by market cap ....")

# sort market capitalization in desceding order
# limit(10) retruns only the first ten records

top_10 = df_clean.orderBy(
    F.desc("market_cap")
).limit(10)

# Display only the required columns
top_10.select(
    "name",
    "symbol",
    "current_price",
    "market_cap"
).show()

# ========================================================
# STEP 7 : DATA VALIDATION
# =======================================================

print("\n [STEP 7] DATA VALIDATION")

# ----------------------------------------------
# 7.1 CHECK FOR NEGATIVE PRICES
# ---------------------------------------------

# Count rows where the current price is below zero
negative_prices = df_clean.filter(
    F.col("current_price") < 0
).count()

# Print a warning if any negative prices are found - f-string
if negative_prices < 0:
    print(f"Warning: Found {negative_prices} records" 
          "with negative prices")
else:
    print("No negative prices found")


# ----------------------------------------------
# 7.2 CHECK FOR ZERO MARKET CAP WITH A POSITIVE PRICE
# ---------------------------------------------

# Find records where market cap is zero but current price is positive 

zero_mcap = df_clean.filter(
    (F.col("market_cap") == 0) &
    (F.col("current_price") > 0)
).count()

# Report the number of potentially inconsitent record

if zero_mcap > 0:
    print(f"warning : Found {zero_mcap} records with zero market cap"
          "but positive price")
else:
    print("No zero-market-cap")


# ========================================================
# STEP 8 : LOADING : SAVE PROCESSED DATA
# =======================================================

print("\n [STEP 8 LOADING - Saving cleaned data ...]")

#-------------------------------------------------------
# 8.1  SAVE THE CLEANED DATA
#------------------------------------------------------

output_path = "output/coingecko_cleaned_pyspark"

df_clean.write.mode("overwrite").csv(
    output_path,
    header=True
)
print(f"Cleaned data saved to : {output_path}")

#-------------------------------------------------------
# 8.2  SAVE AGGREGATE STATISTICS
#------------------------------------------------------

agg_output_path = "output/coingecko_aggregate_pyspark"

category_stats.write.mode("overwrite").csv(
    agg_output_path,
    header=True
)
print(f"Aggregate data saved to : {agg_output_path}")


#-------------------------------------------------------
# 8.3  SAVE THE TOP 10 CRYPTOCURRENCIES
#------------------------------------------------------

top_10_output_path = "output/top10_coins_pyspark"

top_10.select(
    "name",
    "symbol",
    "current_price",
    "market_cap"
).write.mode("overwrite").csv(
    top_10_output_path,
    header=True
)

print(f"Top 1- coins saved to : {top_10_output_path}")

# ====================================================
# STEP 9 : PERFORMANCE MONITORING 
# ====================================================

print("\n [STEP 9]  PERFORMANCE MONITORING...")

# Display the number of partitions in the cleaned DataFrame
# Parititions are units of work spark can process in parellel
# Resilient: Spark can recover lost data partitions using lineage information.
# Distributed: Data can be spread across multiple machines in a cluster.
# Dataset: A collection of records that Spark processes

# print(f"Number of Partitions :"
#        f"{len(df.rdd.glom().collect())}")
    #   f"{df.rdd.getNumPartitions()}")

print("Number of partitions :", df.rdd.getNumPartitions())
print("Number of partitions :", df_clean.rdd.getNumPartitions())


print("\n Query plan:")
df_clean.explain()


# ========================================
# SUMMARY
# ========================================

print("PYSPARK ETL PIPELINE SUMMARY")

print(f"Original records : {df.count}")
print(f"Cleaned records : {df_clean.count()}")
print(f"Records removed : {df.count() - df_clean.count()}")
print(f"Processing mode : Distributed computing")
print(f"Number of partitions : {df_clean.rdd.getNumPartitions}")
print(f"Output files : 3 csv (partitioned)")

spark.stop()
print("Spark session stopped")




