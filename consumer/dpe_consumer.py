import json
import os
from kafka import KafkaConsumer
from minio import Minio
from io import BytesIO

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "host.docker.internal:9092")
TOPIC_NAME = "en-data"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKET_NAME = "datalake"
PREFIX = "bronze"

BATCH_SIZE = 1000
MAX_MESSAGES = 10000

print(f"Connexion Kafka : {KAFKA_BROKER}")
print(f"Connexion MinIO : {MINIO_ENDPOINT}")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

records = []
batch_id = 0
total_read = 0

print("Lecture des messages Kafka (DPE)...")

for message in consumer:
    records.append(message.value)
    total_read += 1

    if len(records) >= BATCH_SIZE:
        batch_id += 1
        object_name = f"{PREFIX}/dpe_batch_{batch_id}.json"
        json_data = "\n".join(json.dumps(r) for r in records)
        buffer = BytesIO(json_data.encode("utf-8"))

        minio_client.put_object(
            BUCKET_NAME,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/json"
        )

        print(f"Batch {batch_id} envoyé → {object_name}")
        records = []

    if total_read >= MAX_MESSAGES:
        break

consumer.close()

print("\n==============================")
print("Consumer DPE terminé")
print(f"Messages traités : {total_read}")
print(f"Batchs créés     : {batch_id}")
print("==============================")