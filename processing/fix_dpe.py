from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder \
    .appName("FixDPE") \
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

# Lire sans multiLine pour lire ligne par ligne
df_raw = spark.read \
    .json("s3a://datalake/bronze/dpe_batch_*.json")

print(f"Colonnes raw : {df_raw.columns[:5]}")
print(f"Lignes raw   : {df_raw.count()}")

# Vérifier si les données sont dans une colonne struct
if len(df_raw.columns) <= 3:
    print("Structure imbriquée détectée → on extrait data.*")
    df_dpe = df_raw.select("data.*") if 'data' in df_raw.columns else df_raw
else:
    df_dpe = df_raw

print(f"Lignes DPE   : {df_dpe.count()}")
print(f"Colonnes DPE : {len(df_dpe.columns)}")
df_dpe.show(2, truncate=True)

# Nettoyage basique
df_dpe = df_dpe.dropna(how="all")
df_dpe = df_dpe.dropDuplicates()

# Supprimer colonnes entièrement vides
exprs = [count(when(col(c).isNotNull(), c)).alias(c) for c in df_dpe.columns]
counts = df_dpe.select(exprs).collect()[0].asDict()
colonnes_non_vides = [c for c, v in counts.items() if v > 0]
df_dpe = df_dpe.select(colonnes_non_vides)

print(f"Lignes après nettoyage  : {df_dpe.count()}")
print(f"Colonnes après nettoyage : {len(df_dpe.columns)}")

# Sauvegarder en silver
df_dpe.repartition(10).write \
    .mode("overwrite") \
    .parquet("s3a://datalake/silver/dpe/")

print("✅ DPE corrigé dans silver/dpe/")
spark.stop()