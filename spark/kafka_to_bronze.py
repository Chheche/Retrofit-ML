import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# configuration Hadoop Windows
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

# ===============================
# SPARK SESSION
# ===============================

spark = (
    SparkSession.builder
    .appName("KafkaToBronze")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.streaming.checkpointLocation", "s3a://data-lake/checkpoints")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===============================
# STREAM DPE
# ===============================

dpe_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "dpe_topic")
    .option("startingOffsets", "earliest")
    .load()
)

dpe_stream = dpe_stream.selectExpr("CAST(value AS STRING)")

dpe_query = (
    dpe_stream.writeStream
    .format("parquet")
    .option("path", "s3a://data-lake/bronze/dpe")
    .option("checkpointLocation", "s3a://data-lake/checkpoints/dpe")
    .outputMode("append")
    .start()
)

# ===============================
# STREAM ENEDIS
# ===============================

enedis_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "enedis_topic")
    .option("startingOffsets", "earliest")
    .load()
)

enedis_stream = enedis_stream.selectExpr("CAST(value AS STRING)")

enedis_query = (
    enedis_stream.writeStream
    .format("parquet")
    .option("path", "s3a://data-lake/bronze/enedis")
    .option("checkpointLocation", "s3a://data-lake/checkpoints/enedis")
    .outputMode("append")
    .start()
)

# garder le streaming actif
spark.streams.awaitAnyTermination()