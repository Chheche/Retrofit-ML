#!/usr/bin/env python3
"""
Etape 2 — Transformation Silver (Bronze -> Silver)

Lit les fichiers NDJSON depuis MinIO/bronze,
nettoie et structure les donnees avec PySpark,
ecrit le resultat en Parquet dans MinIO/silver.

Usage :
    pip install -r requirements.txt
    python silver_transform.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import os

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS",    "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET",    "minioadmin")
BUCKET          = "datalake"
BRONZE_PATH     = f"s3a://{BUCKET}/bronze/"
SILVER_PATH     = f"s3a://{BUCKET}/silver/"


COLONNES_CIBLES = [
    "N_DPE",
    "Date_reception_DPE",
    "Etiquette_DPE",
    "Etiquette_GES",
    "Conso_5_usages_e_finale",
    "Conso_5_usages_par_m2_e_finale",
    "Surface_habitable_logement",
    "Type_batiment",
    "Periode_construction",
    "Type_energie_principale_chauffage",
    "Code_postal_ban",
    "Emission_GES_5_usages_par_m2",
]


def create_spark_session():
    print("Demarrage de la session Spark...")
    spark = (
        SparkSession.builder
        .appName("DPE-Silver-Transform")
        .master("spark://localhost:7077")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark OK")
    return spark


def read_bronze(spark):
    print(f"Lecture des donnees bronze depuis {BRONZE_PATH}...")
    df = spark.read.json(BRONZE_PATH + "*/*/*.json")
    print(f"  Lignes lues : {df.count()}")
    print(f"  Colonnes    : {len(df.columns)}")
    return df


RENAME_MAP = {
    "N°DPE":                               "N_DPE",
    "Date_réception_DPE":                  "Date_reception_DPE",
    "Etiquette_DPE":                       "Etiquette_DPE",
    "Etiquette_GES":                       "Etiquette_GES",
    "Conso_5_usages_é_finale":             "Conso_5_usages_e_finale",
    "Conso_5_usages_par_m²_é_finale":      "Conso_5_usages_par_m2_e_finale",
    "Surface_habitable_logement":          "Surface_habitable_logement",
    "Type_bâtiment":                       "Type_batiment",
    "Période_construction":                "Periode_construction",
    "Type_énergie_principale_chauffage":   "Type_energie_principale_chauffage",
    "Code_postal_ban":                     "Code_postal_ban",
    "Emission_GES_5_usages_par_m²":        "Emission_GES_5_usages_par_m2",
}


def rename_columns(df):
    for old, new in RENAME_MAP.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def clean(df):
    print("Nettoyage des donnees...")

    df = rename_columns(df)

    cols_presentes = [c for c in COLONNES_CIBLES if c in df.columns]
    df = df.select(cols_presentes)

    before = df.count()

    if "N_DPE" in df.columns:
        df = df.dropDuplicates(["N_DPE"])

    numeric_cols = [
        "Conso_5_usages_e_finale",
        "Conso_5_usages_par_m2_e_finale",
        "Surface_habitable_logement",
        "Emission_GES_5_usages_par_m2",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    df = df.filter(
        F.col("Etiquette_DPE").isNotNull() &
        F.col("Conso_5_usages_e_finale").isNotNull() &
        F.col("Conso_5_usages_par_m2_e_finale").isNotNull() &
        F.col("Surface_habitable_logement").isNotNull()
    )

    df = df.filter(F.col("Etiquette_DPE").isin(["A", "B", "C", "D", "E", "F", "G"]))

    df = df.filter(
        (F.col("Conso_5_usages_par_m2_e_finale") >= 0) &
        (F.col("Conso_5_usages_par_m2_e_finale") <= 2000) &   # max raisonnable kWh/m2
        (F.col("Surface_habitable_logement") >= 9) &           # min 9m2
        (F.col("Surface_habitable_logement") <= 1000)          # max 1000m2
    )

    etiquette_map = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7}
    df = df.withColumn(
        "Etiquette_DPE_num",
        F.when(F.col("Etiquette_DPE") == "A", 1)
         .when(F.col("Etiquette_DPE") == "B", 2)
         .when(F.col("Etiquette_DPE") == "C", 3)
         .when(F.col("Etiquette_DPE") == "D", 4)
         .when(F.col("Etiquette_DPE") == "E", 5)
         .when(F.col("Etiquette_DPE") == "F", 6)
         .when(F.col("Etiquette_DPE") == "G", 7)
    )

    df = df.withColumn("_silver_timestamp", F.current_timestamp())

    after = df.count()
    print(f"  Avant nettoyage : {before} lignes")
    print(f"  Apres nettoyage : {after} lignes")
    print(f"  Supprimes       : {before - after} lignes")

    return df


def explore(df):
    print("\n--- Exploration ---")
    print("Schema :")
    df.printSchema()

    print("\nDistribution des etiquettes DPE :")
    df.groupBy("Etiquette_DPE") \
      .agg(
          F.count("*").alias("nb"),
          F.round(F.avg("Conso_5_usages_par_m2_e_finale"), 1).alias("conso_moy_kWh_m2")
      ) \
      .orderBy("Etiquette_DPE") \
      .show()

    print("Conso moyenne par type de batiment :")
    if "Type_batiment" in df.columns:
        df.groupBy("Type_batiment") \
          .agg(F.round(F.avg("Conso_5_usages_par_m2_e_finale"), 1).alias("conso_moy")) \
          .orderBy(F.desc("conso_moy")) \
          .show()


def write_silver(df):
    print(f"\nEcriture Silver dans {SILVER_PATH}...")
    (
        df.write
          .mode("overwrite")
          .partitionBy("Etiquette_DPE")
          .parquet(SILVER_PATH)
    )
    print("Silver ecrit avec succes !")
    print(f"Verifier dans MinIO : http://localhost:9001")
    print(f"  Bucket : datalake/silver/")


def main():
    spark = create_spark_session()

    df_bronze = read_bronze(spark)
    df_silver = clean(df_bronze)
    explore(df_silver)
    write_silver(df_silver)

    spark.stop()
    print("\nTransformation Silver terminee !")

if __name__ == "__main__":
    main()