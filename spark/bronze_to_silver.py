from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# -----------------------------
# Spark Session + MINIO
# -----------------------------

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ) \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 8)

print("Loading Bronze datasets from MinIO")

# -----------------------------
# READ BRONZE (MINIO)
# -----------------------------

dpe_raw = spark.read.parquet("s3a://data-lake/bronze/dpe")
enedis_raw = spark.read.parquet("s3a://data-lake/bronze/enedis")

# -----------------------------
# SCHEMA DPE
# -----------------------------

dpe_schema = StructType([
    StructField("code_insee_ban", StringType(), True),
    StructField("etiquette_dpe", StringType(), True),
    StructField("surface_habitable_logement", DoubleType(), True),
    StructField("type_batiment", StringType(), True),
    StructField("annee_construction", StringType(), True)
])

# -----------------------------
# SCHEMA ENEDIS
# -----------------------------

enedis_schema = StructType([
    StructField("Code Commune", StringType(), True),
    StructField("Nombre de logements", DoubleType(), True),
    StructField("Consommation annuelle totale de l'adresse (MWh)", DoubleType(), True),
    StructField("Année", StringType(), True)
])

# -----------------------------
# PARSE JSON
# -----------------------------

dpe = dpe_raw.withColumn(
    "data",
    from_json(col("value"), dpe_schema)
).select("data.*")

enedis = enedis_raw.withColumn(
    "data",
    from_json(col("value"), enedis_schema)
).select("data.*")

print("JSON parsed")

# -----------------------------
# CLEAN DATA
# -----------------------------

dpe = dpe.dropDuplicates()
enedis = enedis.dropDuplicates()

dpe = dpe.dropna(subset=["code_insee_ban", "etiquette_dpe", "surface_habitable_logement"])
enedis = enedis.dropna(subset=["Code Commune"])

dpe = dpe.filter(
    (col("surface_habitable_logement") > 10) &
    (col("surface_habitable_logement") < 500)
)

# -----------------------------
# FIX TYPES
# -----------------------------

dpe = dpe.withColumn(
    "annee_construction",
    col("annee_construction").cast(IntegerType())
)

# -----------------------------
# AGGREGATE ENEDIS
# -----------------------------

enedis_group = enedis.groupBy("Code Commune").agg(
    sum("Consommation annuelle totale de l'adresse (MWh)").alias("conso_mwh"),
    sum("Nombre de logements").alias("nb_logements")
)

# -----------------------------
# ENERGY CALCULATIONS
# -----------------------------

enedis_group = enedis_group.withColumn(
    "conso_kwh",
    col("conso_mwh") * 1000
)

enedis_group = enedis_group.withColumn(
    "conso_par_logement",
    when(col("nb_logements") > 0, col("conso_kwh") / col("nb_logements"))
)

# -----------------------------
# MERGE
# -----------------------------

df_silver = dpe.join(
    enedis_group,
    dpe["code_insee_ban"] == enedis_group["Code Commune"],
    "inner"
)

# -----------------------------
# ADD FEATURES
# -----------------------------

df_silver = df_silver.withColumn(
    "conso_kwh_m2",
    col("conso_par_logement") / col("surface_habitable_logement")
)

df_silver = df_silver.withColumn(
    "periode_construction",
    when(col("annee_construction") < 1948, "avant_1948")
    .when(col("annee_construction") < 1975, "1948_1975")
    .when(col("annee_construction") < 2000, "1975_2000")
    .otherwise("apres_2000")
)

# -----------------------------
# SAVE SILVER (MINIO)
# -----------------------------

df_silver.write.mode("overwrite").parquet(
    "s3a://data-lake/silver/dpe_enedis"
)

print("Silver dataset created successfully")

df_silver.show(10)

spark.stop()