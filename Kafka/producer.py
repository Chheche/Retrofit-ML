from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------- DATASET 1 : DPE ----------
print("Sending DPE dataset...")

for chunk in pd.read_csv("datasets/dpe03existant.csv", chunksize=1000, low_memory=False):
    
    for _, row in chunk.iterrows():
        producer.send("dpe_topic", row.to_dict())

    print("Sent 1000 rows to dpe_topic")


# ---------- DATASET 2 : ENEDIS ----------
print("Sending ENEDIS dataset...")

for chunk in pd.read_csv("datasets/consommation-annuelle-residentielle-par-adresse.csv", chunksize=1000, low_memory=False):
    
    for _, row in chunk.iterrows():
        producer.send("enedis_topic", row.to_dict())

    print("Sent 1000 rows to enedis_topic")


producer.flush()

print("All datasets sent to Kafka")