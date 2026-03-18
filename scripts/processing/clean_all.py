from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder \
    .appName("CleanAll") \
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

def clean_df(df, nom):
    print(f"\n--- Nettoyage : {nom} ---")
    print(f"Lignes avant   : {df.count()}")
    print(f"Colonnes avant : {len(df.columns)}")
    df = df.dropna(how="all")
    exprs = [count(when(col(c).isNotNull(), c)).alias(c) for c in df.columns]
    counts = df.select(exprs).collect()[0].asDict()
    colonnes_non_vides = [c for c, v in counts.items() if v > 0]
    df = df.select(colonnes_non_vides)
    df = df.dropDuplicates()
    print(f"Lignes après   : {df.count()}")
    print(f"Colonnes après : {len(df.columns)}")
    return df

# =============================================================
# 1. ADDRESSES  DÉJÀ FAIT — commenté
# =============================================================
# df_addr = spark.read \
#     .option("multiLine", True) \
#     .json("s3a://datalake/bronze/addresses_batch_*.json")
# df_addr = clean_df(df_addr, "addresses")
# df_addr.repartition(10).write \
#     .mode("overwrite") \
#     .parquet("s3a://datalake/silver/addresses/")

# =============================================================
# 2. CONSOMMATION (JSON)
# =============================================================
print("\n========== CONSOMMATION ==========")

df_conso = spark.read \
    .option("multiLine", True) \
    .json("s3a://datalake/bronze/consommation-annuelle-residentielle-par-adresse.json")

df_conso = clean_df(df_conso, "consommation")

df_conso.repartition(10).write \
    .mode("overwrite") \
    .parquet("s3a://datalake/silver/consommation/")

print(" Consommation OK")

# =============================================================
# 3. DPE (JSON)
# =============================================================
print("\n========== DPE ==========")

df_dpe = spark.read \
    .option("multiLine", True) \
    .json("s3a://datalake/bronze/dpe_batch_*.json")

df_dpe = clean_df(df_dpe, "dpe")

df_dpe.repartition(10).write \
    .mode("overwrite") \
    .parquet("s3a://datalake/silver/dpe/")

print(" DPE OK")

spark.stop()
print("\n Pipeline terminé avec succès !")