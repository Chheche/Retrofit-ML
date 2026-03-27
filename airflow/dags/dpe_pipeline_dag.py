from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "nadia_bouaicha",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

KAFKA_BROKER = "host.docker.internal:9092"
MINIO_ENDPOINT = "host.docker.internal:9000"

SPARK_CMD = (
    "docker run --rm "
    "-v /opt/airflow/scripts/processing:/scripts "
    "-v /opt/airflow/jars:/jars "
    "-e HOME=/root --user root apache/spark:4.0.2 "
    "/bin/bash -c 'pip install numpy --quiet && "
    "/opt/spark/bin/spark-submit "
    "--jars /jars/aws-java-sdk-bundle-1.12.262.jar,/jars/hadoop-aws-3.3.4.jar "
    "/scripts/{script}'"
)

dag = DAG(
    dag_id="dpe_pipeline",
    default_args=default_args,
    description="Pipeline complet DPE : Kafka → Bronze → Silver → Gold",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dpe", "spark", "kafka", "ml"],
)

kafka_producer_address = BashOperator(
    task_id="kafka_producer_address",
    bash_command=(
        f"cd /opt/airflow/scripts/producer && "
        f"KAFKA_BROKER={KAFKA_BROKER} python address_energy_producer.py"
    ),
    dag=dag,
)

kafka_producer_dpe = BashOperator(
    task_id="kafka_producer_dpe",
    bash_command=(
        f"cd /opt/airflow/scripts/producer && "
        f"KAFKA_BROKER={KAFKA_BROKER} python producer_dpe.py"
    ),
    dag=dag,
)

kafka_consumer_dpe = BashOperator(
    task_id="kafka_consumer_dpe",
    bash_command=(
        f"cd /opt/airflow/scripts/consumer && "
        f"KAFKA_BROKER={KAFKA_BROKER} "
        f"MINIO_ENDPOINT={MINIO_ENDPOINT} python dpe_consumer.py"
    ),
    dag=dag,
)

kafka_consumer_minio = BashOperator(
    task_id="kafka_consumer_minio",
    bash_command=(
        f"cd /opt/airflow/scripts/consumer && "
        f"KAFKA_BROKER={KAFKA_BROKER} "
        f"MINIO_ENDPOINT={MINIO_ENDPOINT} python kafka_to_minio_consumer.py"
    ),
    dag=dag,
)

clean_silver = BashOperator(
    task_id="clean_silver",
    bash_command=SPARK_CMD.format(script="clean_all.py"),
    dag=dag,
)

fix_dpe = BashOperator(
    task_id="fix_dpe",
    bash_command=SPARK_CMD.format(script="fix_dpe.py"),
    dag=dag,
)

fix_consommation = BashOperator(
    task_id="fix_consommation",
    bash_command=SPARK_CMD.format(script="fix_consommation.py"),
    dag=dag,
)

gold_ml = BashOperator(
    task_id="gold_ml",
    bash_command=SPARK_CMD.format(script="gold_ml.py"),
    dag=dag,
)

kafka_producer_address >> kafka_consumer_dpe
kafka_producer_address >> kafka_consumer_minio
kafka_producer_dpe >> kafka_consumer_dpe
kafka_producer_dpe >> kafka_consumer_minio
kafka_consumer_dpe >> clean_silver
kafka_consumer_minio >> clean_silver
clean_silver >> fix_dpe
fix_dpe >> fix_consommation
fix_consommation >> gold_ml