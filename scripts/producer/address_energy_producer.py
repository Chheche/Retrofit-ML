import csv
import json
import os
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "en-data"

CSV_FILE = r"C:\Users\User Lenovo\Downloads\consommation-annuelle-residentielle-par-adresse.csv"

# récupérer le nom du fichier
FILE_NAME = os.path.basename(CSV_FILE)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Lecture du CSV...")

with open(CSV_FILE, encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for i, row in enumerate(reader):

        message = {
            "source_file": FILE_NAME,
            "data": row
        }

        producer.send(TOPIC_NAME, value=message)

        if i % 1000 == 0:
            print(f"{i} lignes envoyées...")

producer.flush()

print("CSV envoyé vers Kafka !")