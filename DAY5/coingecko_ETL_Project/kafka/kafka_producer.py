"""
===================================================
kafka producer - COingecko Live Cryptocurrency data 
==================================================

FLOW :- 
CoinGecko API -> python producer -> Apache kafka topic

1. Fetch cryptocurrency market data from coingecko API.
2. Convert python data into JSON format and then into bytes.
3. Send each cryptocurrency as a seperate message to kafka.
4. Wait 30 seconds and repeat the entire process.

key:value , 'key':'value'

IMPORTANT CONECPTS :
- API :- Used to fetch cryptocurrency market data .
- Producer :- A program that send message to kafka.
- Broker :- The kafka server that receives and stored messages.
- Topics :- A logical channel where kafka messages are stored.
- Message :- A single record containing cryptocurrency information.
- JSON :- (JAVASCRIPT OBJECT NOTATION) - A format used to represent structured data .
- Serialization :- Converting data into a format suitable for transmission.

INSTALL DEPENDECIES :- 
python -m pip install kafka-python requests 

RUN THE FILE :- 
python kafka/kafka_producer.py

REQUIREMENTS :- 
1. kafka must be running at localhost:9092
2. The kafka topic "crypto-prices" must exits . or the broker must be configured to create topics automatically.
===========================================================================
"""

# =========================================================
# PART 1 IMPORT REQUIRED LIBRARIES 
# =========================================================

# Converts python dictionaries into JSON String.
# Kafka messages need to be serialized into bytes before sending
import json

# Provides the sleep() function
# we use it to pause the program between API requests
import time

# Sends HTTP request to the coingecko REST[Representation state tranfer] API.
# we use it to fetch the latest cryptocurrency market data 
import requests

# A class provided by kafka-python
# It allows our python program to publish message to kafka 
from kafka import KafkaProducer


# =========================================================
# PART 2 CONFIGURATION 
# =========================================================

# Address of the kafka broker 
# localhost means kafka is running on the same computer 
# Port 9092 is a common kafka client-listener port
KAFKA_BROKER = "localhost:9092"

# Name of the kafka Topic
# A topic is a logical channel that stored related messages.
# Our cryptocurrency records will be published to this topic
KAFKA_TOPIC = "crypto-prices"

# Number of seconds to wait between sucessive API Requests.
# After fetching and sending batch , the program sleep
# for 30 seconds before sending the next batch 
FETCH_INTERVAL = 30

# CoinGecko's public REST API endpoints for market inforamtion.
# This endpoints returns inforamtion such as coin name, price , 
# market capitilization and 24 hours price changes 
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
# https://www.youtube.com/results?search_query=honey+singh+songs
# =========================================================
# PART 3: CONNECT TO KAFKA 
# =========================================================

# KafkaProducer create a producer object
# This object manages the connetion and send message to kafka
producer=KafkaProducer(

    # bootstrap servers tells the producer where to initially
    # connet to discover the kafka cluster
    bootstrap_servers=KAFKA_BROKER,

    # kafka send message values as bytes.
    # value_serializer automatically convert each python
    # dictionary into a JSON String and then UTF-8 encoded bytes
    # Example :-  python dictionries -> JSON String ->bytes

    # {"name":"Bitcoin"} -> '{"name":"Bitcoin"}' -> b'{"name":"Bitcoin"}'

    value_serializer=lambda data: json.dumps(data).encode("utf-8"),
)

print(f"Connected to kafka at {KAFKA_BROKER}")

print(f"Target Kafka Topic {KAFKA_TOPIC}")

print(f"Fetching data every {FETCH_INTERVAL} seconds \n")

# =========================================================
# PART 4: FETCH DATA FROM COINGECKO API
# =========================================================

def fetch_coins():

    """
     Fetch cryptocurrency market data from coinGecko

     Returns:
     A list of dictionries , with each disctinary representing 
     one cryptocurreny 
    """

    params = {
        "vs_currency":"usd",
        "order":"market_cap_desc",
        "per_page":100,
        "page":1,
        "sparkline":False,  # Do not include sparkline price-history data # This reduces unnecessary response data 
        "price_change_percentage":"24h",
        }

    # Send an HTTP GET request to coinGecko API 
    # params automatically adds the query parameter tp the URL
    # timeout=15 means the request will not wait indefinitely.
    response=requests.get(
        API_URL,
        params=params,
        timeout=15
    )

   # Raise an exection for HTTP errors , such as 429 or 500
   # This prevents us from processing an error message 
    response.raise_for_status() 

   # Convert API's JSON response into python object 
   # a list of dictionaries , one dictionary per cryptocurreny 
    coins=response.json()

    return coins

# =========================================================
# PART 5: SEND CRYPTOCURRENCY DATA TO KAFKA 
# =========================================================

def send_to_kafka(coins):
    """
    send each cryptocurrency as an individual kafka message.

   Args: coins : A list of cryptocurrecny dictinaries .
    """

    for coin in coins:

        producer.send(
            KAFKA_TOPIC,
            key=coin["id"].encode("utf-8"),
            value=coin
        )

    # Flush  = "make sure all message are actually sent" 
    producer.flush()

    print(
        f"sent {len(coins)} coins to kafka topic '{KAFKA_TOPIC}'"
    )

# ===============================================

# PART 6: MAIN LOOP 
# Run forever : fetch -> send -> wait 30 seconds -> repeat
# ctrl+c to stop
# =============================================

batch = 0

print(" PRODUCER STARTED - Ctrl+c to stop")

while True:
    batch += 1 
    print(f"\n [Batch #{batch}]")

    # step 1: fetch from API 
    print("Fetching from coiGecko API....")
    coins = fetch_coins()
    print(f" Got {len(coins)} coins")

    # step 2: show a quick preview/show currency (top 3)
    print(" Preview (top 3):")
    for coin in coins[:3]:
        print(f" {coin['name']:15s} ${coin['current_price']:>12,.2f}") # display the name in the field 15 characters
        f"({coin['price_change_percentage_24h']:+.2f}%24h)"

    # step 3: send all coins to kafka 
    send_to_kafka(coins)

    # step 4 : wait before next batch 
    print(f" Sleeping {FETCH_INTERVAL}s...")
    time.sleep(FETCH_INTERVAL)













#  https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=24h

# https://www.youtube.com/results?search_query=code+with+harry = querypara String 
# https://www.youtube.com/results?search_query=technical+thapa
# https://www.youtube.com/results?search_query=technical+guruji