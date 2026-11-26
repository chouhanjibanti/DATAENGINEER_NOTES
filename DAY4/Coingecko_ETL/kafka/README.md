Static file -> csv file 

Dynamic data -> Kafka 


                     Kafka 
            
        Producer -> Real Time API data fetching - pass to the broker topic
        Broker topic -> data access from the producer / store 
        Consumer -> It will read the data from the broker.



# https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=24h

# https://www.youtube.com/results?search_query=code+with+harry


zookeeper
==============================================
Docker compose - Kafka + zookeeper + kafka UI 
=============================================   - docker compose up -d

PURPOSE :- 

this files starts three services
1. zookeeper -> manages kafka broker coordination
2. kafka -> Handles message production and consumtion
3. Kafka UI -> Provides a browser based dashbaord to monitor kafka topics and message 


==============================================================


Step 1:- start kafka (Docker)

cd kafka
docker-compose up -d - start docker 

docker-compose down

wait 15 seconds for kafka to be ready

-------------------------

Step 2:- Start the Producer 

python kafka/kafka_producer.py

Step 3:- Start the Consumer 

python kafka/kafka_consumer.py


=================================

Installation commands :- 
python -m pip install kafka-python requests pyspark

using the requirement.txt file 
pip install -r requirements.txt



