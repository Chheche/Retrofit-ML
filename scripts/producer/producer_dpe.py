import json
import time
import requests
from kafka import KafkaProducer

API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"

KAFKA_BROKER = "localhost:9092"
TOPIC = "en-data"

PAGE_SIZE = 1000
TARGET = 10000

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sent = 0
page = 1

print("Début récupération des données...")

while sent < TARGET:

    params = {
        "page": page,
        "size": PAGE_SIZE,
        "format": "json"
    }

    response = requests.get(API_URL, params=params)
    data = response.json()

    results = data.get("results", [])

    if not results:
        print("Plus de données disponibles.")
        break

    for record in results:
        producer.send(TOPIC, value=record)
        sent += 1

        if sent >= TARGET:
            break

    print(f"{sent} messages envoyés vers Kafka")

    page += 1
    time.sleep(0.2)

producer.flush()
producer.close()

print("\n==============================")
print("Production terminée")
print("Messages envoyés :", sent)
print("==============================")