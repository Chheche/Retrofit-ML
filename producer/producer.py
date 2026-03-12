#!/usr/bin/env python3

import json
import time
import argparse
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "open-data"
BATCH_SLEEP = 0.01

# URL API ADEME Data Fair - DPE Logements existants (depuis juillet 2021)
# Identifiant dataset : dpe03existant
API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"

def parse_args():
    parser = argparse.ArgumentParser(description="Producer DPE -> Kafka")
    parser.add_argument("--limit", type=int, default=5000,
    help="Nombre max de lignes a envoyer (defaut: 5000)")
    parser.add_argument("--broker", default=KAFKA_BROKER,
                        help="Adresse du broker Kafka")
    return parser.parse_args()

def fetch_dpe_records(total_limit: int):
    """
    Genere des dictionnaires DPE en appelant l'API ADEME par pages.
    L'API Data Fair utilise le curseur 'after' pour paginer (pas 'page').
    """
    page_size = 100
    fetched = 0
    after = None

    print(f"Telechargement des donnees DPE depuis l'API ADEME...")
    print(f"Objectif : {total_limit} lignes\n")

    while fetched < total_limit:
        size = min(page_size, total_limit - fetched)
        params = {"size": size, "format": "json"}
        if after:
            params["after"] = after

        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Erreur HTTP : {e}")
            break

        data = resp.json()
        results = data.get("results", [])

        if not results:
            print("Plus de donnees disponibles.")
            break

        for record in results:
            yield record
            fetched += 1

        after = data.get("after")
        if not after:
            print("Fin du dataset.")
            break

        print(f"{fetched} lignes recuperees...", end="\r")

    print(f"\nTotal telecharge : {fetched} lignes")

def json_serializer(data):
    return json.dumps(data, ensure_ascii=False).encode("utf-8")

def main():
    args = parse_args()

    print(f"Connexion au broker Kafka : {args.broker}")
    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=json_serializer,
        acks="all",
        retries=3,
    )

    sent = 0
    errors = 0

    for record in fetch_dpe_records(args.limit):
        record["_pipeline_source"] = "ademe-dpe03existant"
        record["_pipeline_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        key_field = record.get("N_DPE") or record.get("numero_dpe") or str(sent)
        key = str(key_field).encode("utf-8")

        try:
            future = producer.send(TOPIC_NAME, key=key, value=record)
            future.get(timeout=10)
            sent += 1
        except KafkaError as e:
            print(f"\nErreur Kafka ligne {sent}: {e}")
            errors += 1

        if sent % 100 == 0:
            print(f"{sent} messages envoyes...", end="\r")

        time.sleep(BATCH_SLEEP)

    producer.flush()
    producer.close()

    print(f"\n{'='*50}")
    print(f"Production terminee !")
    print(f"Messages envoyes : {sent}")
    print(f"Erreurs : {errors}")
    print(f"Topic : {TOPIC_NAME}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()