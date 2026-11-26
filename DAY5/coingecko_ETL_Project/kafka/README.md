kafka :- 

work on static - csv 
work on dynamic - API fetching data 

Kafka :- 
Producer -> Data fetching from the APIs
broker topic -> data access from producer 
Consumer -> receive the data from broker 


python -m pip install kafka-python requests

========================================================
# ============================================================
# Docker Compose - Kafka + ZooKeeper + Kafka UI
# ============================================================
#
# PURPOSE:
# This file starts three services:
# 1. ZooKeeper  -> Manages Kafka broker coordination.
# 2. Kafka      -> Handles message production and consumption.
# 3. Kafka UI   -> Provides a browser-based dashboard to
#                  monitor Kafka topics and messages.
#
# START ALL SERVICES:
# docker compose up -d
#
# STOP ALL SERVICES:
# docker compose down
#
# CHECK RUNNING CONTAINERS:
# docker compose ps
#
# VIEW LOGS:
# docker compose logs -f
#
# KAFKA UI:
# http://localhost:8080
#
# KAFKA BROKER:
# localhost:9092


Step1 :- Install python dependecies 
cd coingecko_ETL_Project
python -m pip install kafka-python requests


Step2 : Start kafka (Docker)

