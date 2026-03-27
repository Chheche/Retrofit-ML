from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, avg

spark = SparkSession.builder \
    .appName("FixConso") \
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

# Lire le JSON brut
df_raw = spark.read \
    .option("multiLine", True) \
    .json("s3a://datalake/bronze/consommation-annuelle-residentielle-par-adresse.json")

# data est un STRUCT → on extrait avec data.*
df_conso = df_raw.select("data.*")

print(f"Colonnes : {df_conso.columns}")
print(f"Lignes   : {df_conso.count()}")
df_conso.show(3, truncate=True)

# Sauvegarder en silver
df_conso.write \
    .mode("overwrite") \
    .parquet("s3a://datalake/silver/consommation/")

print("✅ Consommation corrigée dans silver/consommation/")
spark.stop()