import json
import os
from io import BytesIO
from kafka import KafkaConsumer
from minio import Minio

# Kafka configuration
KAFKA_TOPIC = "en-data"
KAFKA_BROKER = "localhost:9092"

# MinIO configuration
MINIO_ENDPOINT = "127.0.0.1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKET_NAME = "datalake"
PREFIX = "bronze"

OUTPUT_FILE = "consommation-annuelle-residentielle-par-adresse.json"

# Connexion MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Vérifier que le bucket existe
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Lecture des messages Kafka...")

data = []
batch_size = 100000
count = 0

for message in consumer:
    data.append(message.value)
    count += 1

    if count % 10000 == 0:
        print(f"{count} messages lus...")

    # Quand on atteint batch_size on upload
    if len(data) >= batch_size:

        json_bytes = json.dumps(data).encode("utf-8")

        object_name = f"{PREFIX}/{OUTPUT_FILE}"

        minio_client.put_object(
            BUCKET_NAME,
            object_name,
            BytesIO(json_bytes),
            length=len(json_bytes)
        )

        print(f"{batch_size} messages envoyés vers MinIO → {object_name}")

        data = []

# upload final si reste
if data:
    json_bytes = json.dumps(data).encode("utf-8")

    object_name = f"{PREFIX}/{OUTPUT_FILE}"

    minio_client.put_object(
        BUCKET_NAME,
        object_name,
        BytesIO(json_bytes),
        length=len(json_bytes)
    )

    print("Upload final terminé")

print("Pipeline Kafka → MinIO terminé")