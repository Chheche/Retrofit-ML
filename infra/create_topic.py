#!/usr/bin/env python3

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME   = "open-data"

def create_topic():
    print(f"Connexion au broker Kafka : {KAFKA_BROKER}")

    for attempt in range(10):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            break
        except Exception as e:
            print(f"Tentative {attempt+1}/10 — {e}")
            time.sleep(3)
    else:
        raise RuntimeError("Impossible de se connecter à Kafka après 10 tentatives.")

    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=3,
        replication_factor=1,
    )
    
    try:
        admin.create_topics([topic])
        print(f"Topic '{TOPIC_NAME}' créé avec succès.")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' existe déjà.")
    finally:
        admin.close()

if __name__ == "__main__":
    create_topic()
