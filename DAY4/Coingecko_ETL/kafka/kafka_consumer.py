"""
=================================================
pyspark kafka consumer
CoinGecko Live streaming ETL
=================================================

1. Connect Spark to kafka 
2. Define JSON Schema
3. Parse kafka message 
4. Clean and Transform data 
5. Analyze and Validate data 
6. Save Result to parquet 

INSTALL : pip install pyspark
RUN : python kafka/kafka_consumer.py

REQUIRES :- 
  docker-compose up -d
  python kafka/kafka_consumer.py
"""

# ==============================================================
# PART 0: IMPORT REQUIRED LIBRARIES 
# =============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ( StructType , StructField , StringType , DoubleType, LongType , IntegerType)


# ==============================================================
# PART 1: CONFIGURATION
# =============================================================

KAFKA_BROKER = "localhost:9092" # kafka server address
KAFKA_TOPIC = "crypto-prices" # kafka topic name 

TRIGGER_EVERY = "30 seconds" # process incoming data every 30 seconds

OUTPUT_PATH = "output/kafka_live_parquet" # Directory for proecessed parquet files 

# ==================================================================
# PART 2 : START SPARK SESSION 
# ==================================================================

# 1. pyspark application  -> SparkSessiob.builder 
# 2. Maven Repository -> Download kafka connector JAR and dependecies 
# 3. spark run time -> Load connector libaries 
# 4. kafka connected -> Spark can read kafka topic

# 1.1 spark.jars.package -> Spark external dependencies download 
# 1.2 org.apache.spark -> Maven group id 
# 1.3 spark:spark-sql-kafka-0-10_2.13 -> Artifact ID / kafka intgreation libaray
# 1.4 3.4.0 -> connector library version

spark = (
    SparkSession.builder
    .appName("coinGeckoKafkaETL") # set spark application name 
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0" # add kafka connector dependecy
    )
    .getOrCreate() # create or reuse spark session
)

spark.sparkContext.setLogLevel("WARN") # Display warning and error logs only


print("Spark Started")
print("Spark Version:", spark.version)

# ==================================================================
# PART 3 :  DEFINE SCHEMA 
# ==================================================================

coin_schema = StructType([
    StructField("id",StringType(),True), # cryptocurrency unique ID 
    StructField("symbol",StringType(),True), # Cryptocurrency symbol
    StructField("name",StringType(),True), # Cryptocurrency name 
    StructField("current_price",DoubleType(),True), # Current coin price 
    StructField("market_cap",LongType(),True), # Total Market Capitalization
    StructField("market_cap_rank",IntegerType(),True), # Market Cap Rank
    StructField("total_volume",LongType(),True), # Total trading volume
    StructField("high_24h",DoubleType(),True), # Highest Price in last 24 hours
    StructField("low_24h",DoubleType(),True), # Lowest price in last 24 hours
    StructField("price_change_percentage_24h",DoubleType(),True), # 24 hours price change 
    StructField("circulating_supply",DoubleType(),True), # Coins currently in curculation
    StructField("total_supply",DoubleType(),True), # Total coin supply
    StructField("max_supply",DoubleType(),True), # Maximum possible coin supply
    StructField("ath",DoubleType(),True), # All time high price
    StructField("ath_date",StringType(),True), # Data of all time high
    StructField("last_updated",StringType(),True), # Last api update timestamp
    StructField("kafka_ingested_at",StringType(),True), # Kafka ingestion timestamp
])

print("Schema Defined") # Display schema creation message 

# ==================================================================
# PART 4 :  CONNECT TO KAFKA (EXTRACT)
# ==================================================================

raw_stream = (
    spark.readStream
    .format("kafka") # Read data from kafka 
    .option("kafka.bootstrap.servers",KAFKA_BROKER) # connet to kafka broker 
    .option("subscribe",KAFKA_TOPIC) # subscribe to crypto-price topic 
    .option("startingOffsets","latest") # Read newly arriving messages
    .option("failOnDataLoss","false") # Continue if some kafka data is unavailable 
    .load() # Load incoming kafka records
)


# kafka stores messages as key-value pairs , the value contains JSON Data 

df_stream = (
    raw_stream.select(
        F.from_json(
            F.col("value").cast("String"), # Convert kafka binary value to string 
            coin_schema  # apply the predefined json schema 
        ).alias("coin")
    )
    .select("coin.*") # Extract all cryptocurrency columns 
)

print("Connected to kafka topic - stream ready ") # Display kafka connetion message 

# ==================================================================
# PART 5:  PROCESS EACH BATCH 
# CLEAN -> TRANSFORM -> ANALYZE -> VALIDATE -> LOAD 
# ==================================================================

def process_batch(batch_df , batch_id): 

    count=batch_df.count() # Count records in the current batch 

    if count == 0:
        print(f"[Batch #{batch_id}] Empty - waiting for data ......") # Display empty batch message 
        return # Stop processing this batch 

    print(f"\n")
    print(f"[Batch #{batch_id}] {count} records from kafka ") # Display batch Id and record count


   
# ================================================
#  STEP 4 : DATA CLEANING  
# ================================================

    print("\n [STEP 4]  DATA CLEANING....") # Display data 

    df = batch_df.na.fill({
    "current_price" : 0,
    "market_cap":0,
    "total_volume" : 0,
    "circulating_supply":0
    })

    # if max_supply is null , use circulating_supply as a fallback 
    # F.coalesce returns the first not null value 
    df=df.withColumn(
     "max_supply",
     F.coalesce(
         F.col("max_supply"),
         F.col("circulating_supply")
     )
    )

    df = df.filter(F.col("current_price") >0 ) # keep records with the positive price

    print(f" After cleaning: {df.count()} records") # Display records remaining after cleaning

# =====================================================
#  STEP 5: DATA TRANSFORMATION - Apply business logic 
# =====================================================

    print("\n [STEP 5] DATA TRANSFORMATION......")

    print("Creating Derived Columns.....")

    # Calculate the ratio of current price to marke capitalization
    # Adding 1 to the denominator avoids division by zero 

    df = df.withColumn(
    "price_to_market_cap_ratio",
    F.col("current_price") / (F.col("market_cap")+1)
    )


    # Calcuate the ratio of trading volume to market capitilization 
    df = df.withColumn(
    "volume_to_market_cap_ratio",
    F.col("total_volume") / (F.col("market_cap")+1)
    )


    print("Categorizing coins by market cap......")

    # F.when works like if/elif in python 
    # F.otherwise specifies the default category 

    df = df.withColumn(
    "market_cap_category",
    F.when(
        F.col("market_cap") >= 100000000000 , "Large Cap"
    ).when(
        F.col("market_cap") >= 10000000000 , "Mid Cap"
    ).when(
        F.col("market_cap") >= 1000000000 , "Small Cap"
    ).otherwise("Micro Cap")
    )

    print("Categorizing Price changes....")

    # Classify coins according to 24 hours price change percentage

    df = df.withColumn(
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

    df.withColumn(
        "processed_at", 
        F.current_timestamp() # Add spark processing timestamp
    )

    df_transformed = df # Store the transformed dataframe

    # ========================================================
      # STEP TOP 10 COINS BY MARKET CAP 
    # ========================================================

    top_10_df = (
        df_transformed
        .orderBy(F.desc("market_cap")) # Sort by market cap from highest to lowest
        .select(
            "name",
            "symbol",
            "current_price",
            "market_cap_category",
            "price_change_category",
            "price_change_percentage_24h"
        )
        .limit(10) # select only the top 10 coins 
    )

    top_10_df.show(truncate=False) # Display top 10 results

    # ========================================================
    #  MARKET CAP CATEGORY SUMMARY
    # ========================================================

    print("\n Market CAP CATEGORY SUMMARY :")

    category_summary = (
        df_transformed
        .groupBy("market_cap_category") # Group coins by market cap category 
        .agg(
            F.count("*").alias("coin_count"), # count coin each category 
            F.round(F.avg("current_price"),2).alias("avg_price"), # calculate average price 
            F.round(F.sum("market_cap") / 1e9 , 2).alias("total_mcap_B") # calculate market cap in billions      
        )
        .orderBy(F.desc("total_mcap_B")) # Sort categories by total market cap
    )

    category_summary.show(truncate=False) # Display category summary 

    # ==================================================================
    #   DATA VALIDATION
    # ==================================================================

    print("\n DATA VALIDATION :")

    neg_prices = df_transformed.filter(
    F.col("current_price") < 0  # Find negative prices
    ).count()  # count negative price records

    zero_mcap = df_transformed.filter(
    (F.col("market_cap") == 0) & # Check for zero market cap
    (F.col("current_price") > 0) # check for positive price 
    ).count() # Count negative with zero market cap

    print(f" Negative Prices :  {neg_prices}") # Display negative price count
    print(f" Zero market cap : {zero_mcap}" ) # Display zero market cap count
    print(f" Records after clean : {df_transformed.count()}") # Display cleaned records count

    # =====================================================
    # SAVE DATA TO PARQUET
    # =====================================================

    print(f"\n SAVING BATCH #{batch_id} -> {OUTPUT_PATH}") # Display save location

    (
        df_transformed.write
        .mode("append") # append data without deleting existing files 
        .partitionBy("market_cap_category") # creates partitions by market cao category
        .parquet(OUTPUT_PATH) # Save data in parquet format 
    )

    print(f" Saved {df_transformed.count()} records") #Display saved records count
    print(f"\n Waiting {TRIGGER_EVERY} for next batch...\n") # Display next batch waiting message 

    # ===========================================================
    #   START STREAMING QUERY
    # ===========================================================

    print("\n Starting Streaming query")

    print(f" Topic  : {KAFKA_TOPIC}") # Diplay kafka topic
    print(f" Output : {OUTPUT_PATH}") # Display parquet output path
    print(f" Trigger: every {TRIGGER_EVERY}") # Display trigger interval 

    print(" Press Ctrl+C to sop\n") # Display stop instruction

query = (
    df_stream.writeStream
    .foreachBatch(process_batch) # Run process batch for every micro-batch
    .trigger(processingTime=TRIGGER_EVERY) # set micro-batch interval 
    .start() # Start streaming query 
)

try:
        query.awaitTermination() # Keep the streaming application running 

except KeyboardInterrupt:  # handle ctrl+c
        print("\nStopped By User") # Display stop message 

finally:
        query.stop()
        spark.stop()
        print("Spark closed cleanly")

    


