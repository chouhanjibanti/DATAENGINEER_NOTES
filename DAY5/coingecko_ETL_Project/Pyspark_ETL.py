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
# from pyspark.sql import funtions as F

# Import spark data types for defining schema when needed
# from pyspark.sql.types import *

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

df=spark.read.csv(
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
#   STEP : 4 DATA CLEANING
# ==========================================================

print("\n [STEP 4] DATA CLENING ...")

# ==============================
#    4.1 HANDLE MISSING VALUES
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
#    4.2  REMOVE DUPLICATE RECORDS
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
#   4.2  REMOVE INVALID PRICE 
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

# Calculate trading volume raltive to market capitalization 
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
    "markrt_cap_category",
    F.when(
        F.col("market_cap") >= 100000000000 , "Large cap"
    ).when(
        F.col("market_cap") >= 10000000000 , "Mid cap"
    ).when(
        F.col("market_cap") >= 1000000000 , "Small cap"
    ).otherwise("Micro Cap")
)