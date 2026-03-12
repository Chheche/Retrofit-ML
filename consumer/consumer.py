#!/usr/bin/env python3

import json
import time
import argparse
import boto3
from datetime import datetime, timezone
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "open-data"
CONSUMER_GROUP = "dpe-bronze-writer"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"
BUCKET_NAME = "datalake"

def parse_args():
    parser = argparse.ArgumentParser(description="Consumer DPE : Kafka → MinIO Bronze")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Nb de messages par fichier (défaut: 500)")
    parser.add_argument("--flush-interval", type=int, default=30,
                        help="Flush toutes les N secondes même si batch incomplet (défaut: 30s)")
    parser.add_argument("--broker", default=KAFKA_BROKER)
    parser.add_argument("--minio", default=MINIO_ENDPOINT)
    return parser.parse_args()

def get_minio_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )

def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' trouvé.")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' créé.")

def write_batch_to_bronze(s3, bucket: str, records: list, batch_index: int) -> str:
    """
    Écrit une liste de records en JSON dans :
      datalake/bronze/date=YYYY-MM-DD/dpe_HHMMSS_<batch>.json
    Retourne la clé S3 du fichier créé.
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M%S")
    s3_key = f"bronze/date={date_str}/dpe_{time_str}_{batch_index:04d}.json"

    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={
            "pipeline-layer": "bronze",
            "pipeline-source": "kafka-open-data",
            "record-count": str(len(records)),
            "write-timestamp": now.isoformat(),
        },
    )
    return s3_key

def main():
    args = parse_args()

    print(f"Connexion à Kafka : {args.broker}")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=args.broker,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=60000,
    )

    print(f"Connexion à MinIO : {args.minio}")
    s3 = get_minio_client(args.minio)
    ensure_bucket(s3, BUCKET_NAME)

    print(f"\nEn écoute sur le topic '{TOPIC_NAME}'...")
    print(f"Batch size : {args.batch_size} messages")
    print(f"Flush interval : {args.flush_interval}s")
    print(f"Destination : s3://{BUCKET_NAME}/bronze/date=YYYY-MM-DD/\n")

    buffer = []
    batch_index = 0
    total_saved = 0
    last_flush = time.time()

    try:
        for message in consumer:
            buffer.append(message.value)

            should_flush = (
                len(buffer) >= args.batch_size
                or (time.time() - last_flush) >= args.flush_interval
            )

            if should_flush and buffer:
                s3_key = write_batch_to_bronze(s3, BUCKET_NAME, buffer, batch_index)
                total_saved += len(buffer)
                print(f"Batch {batch_index:04d} ({len(buffer)} msgs) → s3://{BUCKET_NAME}/{s3_key}")
                buffer = []
                batch_index += 1
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        if buffer:
            s3_key = write_batch_to_bronze(s3, BUCKET_NAME, buffer, batch_index)
            total_saved += len(buffer)
            print(f"Dernier batch ({len(buffer)} msgs) → s3://{BUCKET_NAME}/{s3_key}")

        consumer.close()

    print(f"\n{'='*50}")
    print(f"Consommation terminée !")
    print(f"Total messages sauvegardés : {total_saved}")
    print(f"Fichiers créés : {batch_index + 1}")
    print(f"Bucket MinIO : {BUCKET_NAME}/bronze/")
    print(f"{'='*50}")
    print(f"\nVérifier dans MinIO : http://localhost:9001")
    print(f"Login : minioadmin / minioadmin")

if __name__ == "__main__":
    main()