"""
================================================================
Kafka Producer - CoinGecko Live Cryptocurrency Data 
================================================================

Flow -> CoinGecko API -> python producer -> Apache kafka Topic

1. Fecth Cryptocurrency market data from coinGecko API.
2. Convert Python data into JSON Format and then into bytes.
3. Send each cryptocurrency as a seperarte message to kafka.
4. Wait 30 seconds and repeat the entire process.

IMPORTANT CONCEPTS :- 
- API :- used to fetch cryptocurrency market data.
- Producer :- A program that sends messages to kafka.
- Broker :- The kafka server that receives and stored messages.
- Topic :- A logical channel where kafka messages are stored.
- Message :- A single record containing cryptocurrency information.
- JSON :- (javascript object notation) - A format used to represent structured data.
- Serialization :- Converting data into a foramt suitable for transmission.

INSTALL DEPENDECIES:
python -m pip install kafka-python requests

RUN THE FILE :- 
python kafka/kafka_producer.py

REQUIREMENTS :
1. Kafka must be running at localhost:9092
2. The kafka topic "crypto-prices" must exist, or the broker must be configured to create topics automatically.
=====================================================================
"""

# ===========================================================
# PART 1: IMPORT REQUIRED LIBRARIES 
# ==========================================================

# Converts python dictionaries into JSON Strings.
# kafka messages need to be serialized into bytes before sending.
import json

# Provides the sleep() function
# we use it to pause the program between API requests.
import time

# Send HTTP request to the coinGecko REST API.
# we use it to fetch the latest cryptocurrency market data
import requests


# A class provided by kafka-python
# It allows our python program to publish message to kafka 
from kafka import KafkaProducer

# ===========================================================
# PART 2: CONFIGURATION 
# ==========================================================

# Address of kafka broker
# localhost means kafka is running on the same computer
# Port 9092 is a common kafka client-listener port.
KAFKA_BROKER = "localhost:9092"


# Name of the kafka topic.
# A topic is a logical channel that stores realted messages.
# Our cryptocurrency records will be published to this topic.
KAFKA_TOPIC = "crypto-prices"

# Number of seconds ti wait between sucessessive api request.
# After fetching and sending one batch , the program sleeps.
# for 30 seconds before starting the next batch.
FETCH_INTERVAL = 30

# CoinGecko's public REST API endpoints for market information.
# This endpoints returns information such as coin name , price 
# market capitalization and 24 hours price change.
API_URL = "https://api.coingecko.com/api/v3/coins/markets"


# ===========================================================
# PART 3: CONNECT TO KAFKA 
# ==========================================================

# kafkaProducer created a producer object.
# This object manages the connection and send messages to kafka 
producer=KafkaProducer(

    # bootstrap_servers tells the producer where to initially
    # connect to discover the kafka cluster 
    bootstrap_servers=KAFKA_BROKER,

    value_serializer=lambda data: json.dumps(data).encode("utf-8"),

    # key_serilizer is not provided because we mannully convert
    # each message key into bytes inside sent_to_kafka()   
)

print(f"Connected to kafka at {KAFKA_BROKER}")

print(f"Target kafka topic : '{KAFKA_TOPIC}'")

print(f"Fetching data every {FETCH_INTERVAL} seconds\n")

# ===========================================================
# PART 4: FETCH DATA FROM COINGECKO API  
# ==========================================================

def fetch_coins():
    """
     Fetch cryptocurrency market data from coinGecko

     Returns:
         A list of dictionries , with each dictionary representing one cryptocurrency.
         https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=24h
    """

    params = {
        "vs_currency": "usd", # Return cryptocurrency prices in US Dollars.
        "order":"market_cap_desc",
        "per_page":100,
        "page":1,
        "sparkline":False, # Do not include sparkline price-history data 
    }

   # Send an HTTP GET request to the coingecko API.
   # params automatically adds the query parameter to the URL.
   # timeout=15 means the request will not wait.
    response=requests.get(
         API_URL,
         params=params,
         timeout=15
    )

    # Raise an exception for HTTP error , such as 429 or 500.
    # This prevents us from silently processing an error response.
    response.raise_for_status()

    # Convert the API's JSON response into python objects.
    coins=response.json()

    return coins


# ===========================================================
# PART 5: SEND CRYPTOCURRENCY DATA TO KAFKA 
# ==========================================================

def send_to_kafka(coins):
    """
    Send each cryptocurrency as an individual kafka message 

    Args:
      coins : A list of cryptocurrency dictionaries
    """

    for coin in coins:
        # Each coin become one kakfa message 
        # key = coin id 
        # value = the full coin dict (auto converted to JSON bytes)
        producer.send(
            KAFKA_TOPIC,
            key= coin["id"].encode("utf-8"),
            value =coin
        )

    # Flush :- "Make sure all message are actually sent"
    producer.flush()
    print(f" Sent {len(coins)} coins to kafka topics '{KAFKA_TOPIC}'")

# ============================================================
# PART 6: MAIN LOOP
# Run Forever :- fetch -> send -> wait 30 seconds -> repeat 
# ============================================================

batch = 0

print("PRODUCER STARTED - ctrl+c to stop")

while True:
    batch += 1
    print(f"\n[Batch #{batch}]")

    # step 1: fetch from API
    print("Fetching from coinGecko API....")
    coins=fetch_coins()
    print(f"Got {len(coins)} coins")

    # step 2: show a quick preview (top 3)
    print(" Preview (top 3)")
    for coin in coins[:3]: 
        print(f": {coin['name']:15s} ${coin['current_price']:12,.2f}" f"({coin['price_change_percentage_24h']:+.2f}%24h)")
        # ${coin['current_price']:>12,.2f - right align in a field 12 characters wide. = display two digits after the decimal point
        # {coin['price_change_percentage_24h']:+.2f}%24h) :- display the sign for positive and negative values
    
    # step 3: send all coins to kafka 
    send_to_kafka(coins)

    # step 4 : wait before next fetch
    print(f" Sleeping {FETCH_INTERVAL}")
    time.sleep(FETCH_INTERVAL)
     

