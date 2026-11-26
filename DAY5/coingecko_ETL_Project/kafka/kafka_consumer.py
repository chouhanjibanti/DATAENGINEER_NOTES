"""
======================================================
Pyspark Kafka Consumer 
Coingecko Live streaming ETL 
=====================================================

1. Connect spark to kafka
2. Define JSON Schema
3. parse kafka message 
4. Clean and Transform Data 
5. Analyze adn validate data 
6. save results to parquet

INSTALL :- pip install pyspark 
RUN :- python kafka/kafka_consumer.py

REQUIRES :- docker-compose up -d
             python kafka/kafka_consumer.py
      
"""

# =========================================================
# PART 0 IMPORT REQUIRED LIBRARIES 
# =========================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ( StructType , StructField , StringType , DoubleType , LongType, IntegerType)

# =========================================================
# PART 1 CONFIGURATION 
# =========================================================

KAFKA_BROKER = "localhost:9092" # kafka server address
KAFKA_TOPIC = "crypto-prices" # kakfa topic name 

TRIGGER_EVERY = "30 seconds" # process incoming data every 30 seconds

OUTPUT_PATH = "output/kafka_live_parquet" # directory for processed parquet files

# =========================================================
# PART 2 START SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("CoinGeckoKafkaETL") # Set Spark application name 
    .config(
        "spark.jars.packages", # jar file download 
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0" # add kafka connector dependency 
    )
    .getOrCreate() # create or reuse Spark session
)

spark.sparkContext.setLogLevel("WARN") # Display warning and error logs only

print("Spark Started")

# =========================================================
# PART 3 DEFINE SCHEMA
# =========================================================

coin_schema = StructType([
    StructField("id",StringType(),True), # Cryptocurrency id 
    StructField("symbol",StringType(),True), # Cryptocurrency symbol
    StructField("name",StringType(),True),
    StructField("current_price",DoubleType(),True), # current coin price 
    StructField("market_cap",LongType(),True), # totla market capitlization
    StructField("market_cap_rank",IntegerType(),True), # market cap ranking
    StructField("total_volume",LongType(),True), # total trading volume
    StructField("high_24h",DoubleType(),True), # Highest price in last 24 hours
    StructField("low_24h",DoubleType(),True), # lowest price in last 24 hours
    StructField("price_change_percentage_24h",DoubleType(),True),
    StructField("circulating_supply",DoubleType(),True),
    StructField("total_supply",DoubleType(),True), # Total coin supply 
    StructField("max_supply",DoubleType(),True), # Maximum possible coin supply 
    StructField("ath",DoubleType(),True), # All time high price
    StructField("ath_date",StringType(),True),# Data of all time high
    StructField("last_updated",StringType(),True), # Last Api update timestamp
    StructField("kafka_ingested_at",StringType(),True), # kafka ingestion timestamp
])

print("Schema Defined")

# =========================================================
# PART 4 CONNECT TO KAFKA (EXTRACT)
# =========================================================

raw_stream = (
    spark.readStream
    .format("kafka") # Read data from kafka
    .option("kafka.bootstrap.servers",KAFKA_BROKER) # connet to kafka broker 
    .option("subscribe",KAFKA_TOPIC) # subscribe to crypto-price topic 
    .option("startingOffsets","latest")  # read newly arriving message 
    .option("failOnDataLoss","false")# Continue if some kafka data is unavailable 
    .load() # load incoming kafka records
)

# kafka stored message a key-value pair : the value container json DATA 

df_stream = (
    raw_stream.select(
        F.from_json(
            F.col("value").cast("string"), # convert kafka binary value to string
            coin_schema # Apply the predefined JSON schema 
        ).alias("coin") # Name the parsed JSON Columns
    )
    .select("coin.*") # Extract all cryptocurrency columns 
)

print("Connected to kafka topic - stream ready ") # Display kafka connection message 

# =========================================================
# PART 5 PROCESS EACH BATCH
# CLEAN _> TRANSFORM > ANALYZE > VALIDATE -> LOAD
# =========================================================

def process_batch(batch_df , batch_id):

    count=batch_df.count() # count records in the current batch 

    if count == 0:
        print(f"[Batch #{batch_id}] Empty - waiting for data...")
        return # stop processing this batch

    print(f"[Batch #{batch_id}] {count} records from kafka") 


    # ==========================================================
    #    STEP : 3 EDA (EXPLORATORY DATA ANAYLSIS)
    # ==========================================================

    print("\n [STEP 3 ] EXPLORATORY DATA ANAYLSIS ...")

    # count rows and indentify the number of column 
    print(f"Data Shape : {batch_df.count()} rows , {len(batch_df.columns)} Columns")

    # Display column name and their spark data types
    batch_df.printSchema()

    # Calculate description Statistics for supprted column 
    # Example include count , mean , standard deviation ,minimum and maximum 
    print("\n Basic Statisctics :")
    batch_df.describe().show()

    # Count null values seperatly for every column
    print("\n Missing values per Column :")

    batch_df.select([
        F.count(
            F.when(F.col(c).isNull(),1)
            ).alias(c)
            for c in batch_df.columns
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
    df = batch_df.na.fill({
        "current_price":0,
        "market_cap":0,
        "total_volume":0,
        "circulating_supply":0
    })

    # If max_supply is NULL , use circulating_supply as a fallback 

    df = df.withColumn(
        "max_supply",
        F.coalesce(
            F.col("max_supply"),
            F.col("circulating_supply")
        ))

    df = df.filter(F.col("current_price")>0)

    print(f" After Clening : {df.count()} records")  # Display records remaining after cleaning



        # ==================================================
    # STEP -5 : DATA TRANSFORMATION - APPLY BUSINESS LOGIC  
    # ==================================================

    print("\n [STEP 5] DATA TRANSFORMATION...")

    # ==================================================
    # STEP -5.1 : CREATE DERIVED COLUMNS
    # ==================================================

    # Calcuate the ratio of current price to maket capitalization 
    # adding 1 to the denomination avoids division by zero 

    df = df.withColumn(
        "price_to_market_cap_ratio",
        F.col("current_price") / (F.col("market_cap")+1)
    )

    # Calculate trading volume reltive to market capitalization 
    # This ratio can help compare trading activity across cryptocurrencies
    df = df.withColumn(
        "volume_to_market_cap_ratio",
        F.col("total_volume") / (F.col("market_cap")+1)
    )

    # ==================================================
    # STEP 5.2 CATEGORIZE CRYPTOCURRENCIES BY MARKET CAP
    # ==================================================

    print("categorizing coins by market cap...")

    df = df.withColumn(
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

    df = df.withColumn(
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

    df=df.withColumn(
        "processed_at",
        F.current_timestamp() # add spark processing timestamp
    )

    df_transformed=df



    #------------------------------------------------
    # 6.1 CALCULATE STATISTICS BY MARKET CAP CATEGORY
    #-----------------------------------------------
    print("Aggregating data by maket cap category...")


    # group by create group of cryptocurrencies with the same category
    # agg calcuates summary statistics for each group

    category_stats = df.groupBy(
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

    top_10_df = (
         df_transformed
         .orderBy(F.desc("market_cap")) # Sort by market cap from highest to lowest
         .select(
             "name",
             "id",
             "symbol",
             "current_price",
             "market_cap_category",
             "price_change_percentage_24h",
             "price_change_category"
         )
         .limit(10)
    )

    top_10_df.show(truncate=False)

    # =========================================================
    # MARKET CAP CATEGORY SUMMARY
    # =======================================================

    print("\n MARKET CAP CATEGORY SUMMARY :")

    category_summary = (
        df_transformed
        .groupBy("market_cap_category")
        .agg(
            F.count("*").alias('coin_count'),
            F.round(F.avg("current_price"),2).alias("avg_price"),
            F.round(F.sum("market_cap") / 1e9 ,2).alias("total_mcap_B") # calculate market cap in billion
        )
        .orderBy(F.desc("total_mcap_B"))
    )

    category_summary.show(truncate=False)

    # ======================================================
    # DATA VALIDATION 
    # ======================================================


    print("DATA VALIDATION:")

    neg_price = df_transformed.filter(
        F.col("current_price") < 0  # Find negative prices
    ).count() # cunt negative price records

    zero_mcap = df_transformed.filter(
        (F.col("market_cap") == 0 ) & # check for zero market cap
        (F.col("current_price") > 0 ) # Check for positive price
    ).count() # count records with zero market cap 

    print(f" negative price : {neg_price}")
    print(f" Zero market ca : {zero_mcap}")
    print(f" Records after clean : {df_transformed.count()}")

    # ===========================================================
    # DATA SAVED TO PARQUET
    # ==========================================================

    print(f" \n SAVING BATCH #{batch_id} -> {OUTPUT_PATH}")

    (
        df_transformed.write
        .mode("append") # Append data without deleting the existing files
        .partitionBy("market_cap_category") # Create partition by market cap category
        .parquet(OUTPUT_PATH)
    )

    print(f" Saved {df_transformed.count()} Records")
    print(f"\n Waiting {TRIGGER_EVERY} for next batch....\n")


# ========================================================
# START STREAMING QUERY
# ========================================================

print("\n Starting Streaming Query ....")

print(f"  Topic : {KAFKA_TOPIC}") # Display kafka topic
print(f" Output : {OUTPUT_PATH}")
print(f" Trigger : every {TRIGGER_EVERY}") # Display trigger interval 

print(" Press Ctrl+C to stop\n")

query = (
    df_stream.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime=TRIGGER_EVERY)
    .start() # start streaming query 
)

try:
    query.awaitTermination() # keep the streaming application running 
except KeyboardInterrupt: # Handle Ctrl+c
    print("\n stopped by user")

    query.stop()
    spark.stop()

    print("Spark closed sucessfully....")






        











