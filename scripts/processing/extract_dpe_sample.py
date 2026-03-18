from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExtractDPE") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000") \
    .config("spark.hadoop.fs.s3a.threads.max", "10") \
    .config("spark.hadoop.fs.s3a.max.total.tasks", "5") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("multiLine", True) \
    .json("s3a://datalake/bronze/dpe_batch_1.json")

print(f"Nombre de colonnes : {len(df.columns)}")
print(f"Colonnes : {df.columns}")

# Sauvegarde 10 lignes en CSV local dans /scripts
df.limit(10).coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/scripts/dpe_sample")

print(" Extrait sauvegardé dans /scripts/dpe_sample/")
spark.stop()