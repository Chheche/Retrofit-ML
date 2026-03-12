#!/usr/bin/env python3

import boto3
import json
from datetime import datetime, timezone

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"
BUCKET_NAME = "datalake"

def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )

    print(f"Contenu du bucket '{BUCKET_NAME}' — couche Bronze\n")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="bronze/")

    total_files = 0
    total_records = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key  = obj["Key"]
            size = obj["Size"]

            head = s3.head_object(Bucket=BUCKET_NAME, Key=key)
            meta = head.get("Metadata", {})
            count = meta.get("record-count", "?")

            print(f"{key}")
            print(f"Taille : {size:,} octets | Records : {count}")

            if count.isdigit():
                total_records += int(count)
            total_files += 1

    print(f"\n{'='*55}")
    print(f"Fichiers dans bronze : {total_files}")
    print(f"Records totaux : {total_records:,}")
    print(f"{'='*55}")
    print(f"\nConsole MinIO : http://localhost:9001 (minioadmin/minioadmin)")

    if total_files == 0:
        print("\nAucun fichier trouvé. Vérifier que le consumer a bien tourné.")

if __name__ == "__main__":
    main()
