#!/usr/bin/env python3
"""
Etape 2 - Transformation Silver (Bronze -> Silver)

Lit les fichiers NDJSON depuis MinIO/bronze,
nettoie et structure les donnees avec PySpark,
ecrit le resultat en Parquet dans MinIO/silver.

Usage :
    pip install -r requirements.txt
    python silver_transform.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")
BUCKET         = "datalake"
BRONZE_PATH    = f"s3a://{BUCKET}/bronze/"
SILVER_PATH    = f"s3a://{BUCKET}/silver/"

# Colonnes exactes telles que retournees par l'API ADEME (verifiees dans l'EDA)
COLONNES_CIBLES = [
    "etiquette_dpe",
    "etiquette_ges",
    "surface_habitable_logement",
    "type_batiment",
    "periode_construction",
    "type_energie_principale_chauffage",
    "code_postal_ban",
    "date_reception_dpe",
    "conso_5_usages_par_m2_ef",
]

# Spark Session
def create_spark_session():
    print("Demarrage de la session Spark...")
    spark = (
        SparkSession.builder
        .appName("DPE-Silver-Transform")
        .master("local[*]")
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


# Lecture Bronze
def read_bronze(spark):
    print(f"Lecture des donnees bronze depuis {BRONZE_PATH}...")
    df = spark.read.json(BRONZE_PATH + "*/*.json")
    print(f"  Lignes lues  : {df.count()}")
    print(f"  Colonnes     : {len(df.columns)}")
    print(f"  Apercu colonnes : {df.columns[:10]}")
    return df


# Nettoyage
def clean(df):
    print("\nNettoyage des donnees...")

    # Garder uniquement les colonnes utiles presentes dans le df
    cols_presentes = [c for c in COLONNES_CIBLES if c in df.columns]
    cols_manquantes = [c for c in COLONNES_CIBLES if c not in df.columns]
    print(f"  Colonnes trouvees  : {cols_presentes}")
    if cols_manquantes:
        print(f"  Colonnes absentes  : {cols_manquantes}")
    df = df.select(cols_presentes)

    before = df.count()

    # Cast des colonnes numeriques
    numeric_cols = ["conso_5_usages_par_m2_ef", "surface_habitable_logement"]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # Supprimer les lignes sans etiquette DPE ou sans consommation
    filters = F.col("etiquette_dpe").isNotNull()
    if "conso_5_usages_par_m2_ef" in df.columns:
        filters = filters & F.col("conso_5_usages_par_m2_ef").isNotNull()
    if "surface_habitable_logement" in df.columns:
        filters = filters & F.col("surface_habitable_logement").isNotNull()
    df = df.filter(filters)

    # Garder uniquement les etiquettes valides A B C D E F G
    df = df.filter(F.col("etiquette_dpe").isin(["A", "B", "C", "D", "E", "F", "G"]))

    # Filtrer les valeurs aberrantes
    if "conso_5_usages_par_m2_ef" in df.columns:
        df = df.filter(
            (F.col("conso_5_usages_par_m2_ef") >= 0) &
            (F.col("conso_5_usages_par_m2_ef") <= 2000)
        )
    if "surface_habitable_logement" in df.columns:
        df = df.filter(
            (F.col("surface_habitable_logement") >= 9) &
            (F.col("surface_habitable_logement") <= 1000)
        )

    # Ajouter colonne numerique pour l'etiquette (utile pour ML)
    df = df.withColumn(
        "etiquette_dpe_num",
        F.when(F.col("etiquette_dpe") == "A", 1)
         .when(F.col("etiquette_dpe") == "B", 2)
         .when(F.col("etiquette_dpe") == "C", 3)
         .when(F.col("etiquette_dpe") == "D", 4)
         .when(F.col("etiquette_dpe") == "E", 5)
         .when(F.col("etiquette_dpe") == "F", 6)
         .when(F.col("etiquette_dpe") == "G", 7)
    )

    # Ajouter colonne de date de traitement
    df = df.withColumn("_silver_timestamp", F.current_timestamp())

    after = df.count()
    print(f"  Avant nettoyage : {before} lignes")
    print(f"  Apres nettoyage : {after} lignes")
    print(f"  Supprimes       : {before - after} lignes")

    return df


# Exploration rapide
def explore(df):
    print("\n--- Exploration Silver ---")
    df.printSchema()

    print("\nDistribution des etiquettes DPE :")
    agg_cols = [F.count("*").alias("nb")]
    if "conso_5_usages_par_m2_ef" in df.columns:
        agg_cols.append(
            F.round(F.avg("conso_5_usages_par_m2_ef"), 1).alias("conso_moy_kWh_m2")
        )
    df.groupBy("etiquette_dpe").agg(*agg_cols).orderBy("etiquette_dpe").show()

    if "type_batiment" in df.columns:
        print("Conso moyenne par type de batiment :")
        df.groupBy("type_batiment") \
          .agg(F.round(F.avg("conso_5_usages_par_m2_ef"), 1).alias("conso_moy")) \
          .orderBy(F.desc("conso_moy")) \
          .show()


# Ecriture Silver
def write_silver(df):
    print(f"\nEcriture Silver dans {SILVER_PATH}...")
    (
        df.write
          .mode("overwrite")
          .partitionBy("etiquette_dpe")
          .parquet(SILVER_PATH)
    )
    print("Silver ecrit avec succes !")
    print(f"Verifier dans MinIO : http://localhost:9001")
    print(f"  Bucket : datalake/silver/")


# Main
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