import csv
import json
import os
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "host.docker.internal:9092")
TOPIC_NAME = "addresses-data"

# Limite pour demo
MAX_RECORDS = 1000

# Fichier CSV addresses
CSV_FILE = "/opt/airflow/scripts/data/adresses-01.csv"
FILE_NAME = os.path.basename(CSV_FILE)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Connexion Kafka : {KAFKA_BROKER}")
print(f"Lecture de : {CSV_FILE}")
print(f"Envoi limité à {MAX_RECORDS} enregistrements pour demo...")

total_sent = 0

with open(CSV_FILE, encoding="utf-8") as file:
    reader = csv.DictReader(file, delimiter=';')
    for row in reader:
        if total_sent >= MAX_RECORDS:
            break
        message = {
            "source_file": FILE_NAME,
            "data": row
        }
        producer.send(TOPIC_NAME, value=message)
        total_sent += 1
        if total_sent % 100 == 0:
            print(f"{total_sent} adresses envoyées...")

producer.flush()
producer.close()

print(f"\n==============================")
print(f"Producer Addresses terminé")
print(f"Messages envoyés : {total_sent}")
print(f"==============================")