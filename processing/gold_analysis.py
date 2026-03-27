#!/usr/bin/env python3
"""
Etape 3 — Analyse Gold + Machine Learning

Lit les donnees Silver depuis MinIO,
calcule les gains kWh par saut de classe DPE,
entraine un modele de regression (Random Forest),
stocke les resultats dans MinIO/gold.

Usage :
    pip install -r requirements.txt
    python gold_analysis.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")
BUCKET         = "datalake"
SILVER_PATH    = f"s3a://{BUCKET}/silver/"
GOLD_PATH      = f"s3a://{BUCKET}/gold/"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DPE-Gold-ML")
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


def compute_gain_table(df):
    """
    Calcule pour chaque classe DPE :
    - Conso moyenne en kWh/m2/an
    - Gain moyen si on passe a la classe superieure
    """
    print("\n=== TABLE DES GAINS PAR CLASSE DPE ===")

    stats = (
        df.groupBy("Etiquette_DPE", "Etiquette_DPE_num")
          .agg(
              F.count("*").alias("nb_logements"),
              F.round(F.avg("Conso_5_usages_par_m2_e_finale"), 1).alias("conso_moy_m2"),
              F.round(F.avg("Conso_5_usages_e_finale"), 0).alias("conso_moy_totale"),
              F.round(F.avg("Surface_habitable_logement"), 1).alias("surface_moy"),
              F.round(F.stddev("Conso_5_usages_par_m2_e_finale"), 1).alias("conso_std"),
          )
          .orderBy("Etiquette_DPE_num")
    )
    stats.show()

    # Sauvegarder la table des stats
    stats.write.mode("overwrite").parquet(GOLD_PATH + "stats_par_classe/")
    print("Stats sauvegardees dans gold/stats_par_classe/")
    return stats


def compute_gain_matrix(df):
    """
    Matrice des gains : pour chaque paire (classe_origine, classe_cible)
    calcule le gain moyen en kWh/m2/an et en kWh/an total.
    """
    print("\n=== MATRICE DES GAINS (kWh/m2/an) ===")

    # Conso moyenne par classe
    avg_by_class = (
        df.groupBy("Etiquette_DPE")
          .agg(F.avg("Conso_5_usages_par_m2_e_finale").alias("conso_moy"))
          .collect()
    )
    conso_map = {row["Etiquette_DPE"]: round(row["conso_moy"], 1) for row in avg_by_class}

    classes = ["A", "B", "C", "D", "E", "F", "G"]
    print(f"{'':>6}", end="")
    for c in classes:
        print(f"{'→'+c:>10}", end="")
    print()

    rows = []
    for src in classes:
        print(f"De {src:>2} ", end="")
        for tgt in classes:
            if src == tgt or classes.index(src) <= classes.index(tgt):
                gain = "-"
            else:
                gain_val = conso_map.get(tgt, 0) - conso_map.get(src, 0)
                gain = f"{gain_val:+.0f}"
            print(f"{gain:>10}", end="")
            if gain != "-":
                rows.append({
                    "classe_origine": src,
                    "classe_cible": tgt,
                    "gain_kWh_m2": float(gain) if gain != "-" else None
                })
        print()

    # Sauvegarder la matrice
    from pyspark.sql import Row
    gain_rows = [Row(**r) for r in rows if r["gain_kWh_m2"] is not None]
    df_gains = df.sparkSession.createDataFrame(gain_rows)
    df_gains.write.mode("overwrite").parquet(GOLD_PATH + "matrice_gains/")
    print("\nMatrice sauvegardee dans gold/matrice_gains/")

    return conso_map


def train_model(df):
    """
    Objectif : predire la consommation kWh/m2/an d'un logement
    en fonction de ses caracteristiques.
    Cela permet ensuite d'estimer le gain si on change de classe.
    """
    print("\n=== ENTRAINEMENT DU MODELE DE REGRESSION ===")

    # Features disponibles
    feature_cols   = []
    cat_cols       = []
    numeric_cols   = []

    if "Surface_habitable_logement" in df.columns:
        numeric_cols.append("Surface_habitable_logement")
    if "Etiquette_DPE_num" in df.columns:
        numeric_cols.append("Etiquette_DPE_num")
    if "Type_batiment" in df.columns:
        cat_cols.append("Type_batiment")
    if "Periode_construction" in df.columns:
        cat_cols.append("Periode_construction")
    if "Type_energie_principale_chauffage" in df.columns:
        cat_cols.append("Type_energie_principale_chauffage")

    # Supprimer les lignes avec valeurs manquantes sur les features
    df_ml = df.select(
        numeric_cols + cat_cols + ["Conso_5_usages_par_m2_e_finale"]
    ).dropna()

    print(f"Donnees pour ML : {df_ml.count()} lignes")

    # Pipeline de preprocessing
    stages = []
    encoded_cols = []

    for cat_col in cat_cols:
        indexer  = StringIndexer(inputCol=cat_col, outputCol=cat_col+"_idx",
                                  handleInvalid="keep")
        encoder  = OneHotEncoder(inputCol=cat_col+"_idx", outputCol=cat_col+"_enc")
        stages  += [indexer, encoder]
        encoded_cols.append(cat_col+"_enc")

    all_features = numeric_cols + encoded_cols
    assembler = VectorAssembler(inputCols=all_features, outputCol="features",
                                handleInvalid="keep")
    stages.append(assembler)

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="Conso_5_usages_par_m2_e_finale",
        numTrees=50,
        maxDepth=8,
        seed=42,
    )
    stages.append(rf)

    pipeline = Pipeline(stages=stages)

    # Split train/test
    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    print(f"Train : {train_df.count()} | Test : {test_df.count()}")

    # Entrainement
    print("Entrainement en cours...")
    model = pipeline.fit(train_df)

    # Evaluation
    predictions = model.transform(test_df)
    evaluator_rmse = RegressionEvaluator(
        labelCol="Conso_5_usages_par_m2_e_finale",
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="Conso_5_usages_par_m2_e_finale",
        predictionCol="prediction",
        metricName="r2"
    )
    rmse = evaluator_rmse.evaluate(predictions)
    r2   = evaluator_r2.evaluate(predictions)

    print(f"\n--- Metriques du modele ---")
    print(f"  RMSE : {rmse:.2f} kWh/m2/an")
    print(f"  R2   : {r2:.4f}")
    print(f"  (R2=1 = parfait, R2=0 = inutile)")

    # Sauvegarder les predictions
    predictions.select(
        "Surface_habitable_logement",
        "Etiquette_DPE_num",
        "Conso_5_usages_par_m2_e_finale",
        "prediction"
    ).write.mode("overwrite").parquet(GOLD_PATH + "predictions/")
    print("Predictions sauvegardees dans gold/predictions/")

    # Sauvegarder le modele
    model.write().overwrite().save(GOLD_PATH + "model_rf/")
    print("Modele sauvegarde dans gold/model_rf/")

    return model, rmse, r2


def build_dashboard_table(df, conso_map):
    """
    Cree une table agrégee prete pour Streamlit :
    - Gain par saut de classe
    - Par type de batiment
    - Par periode de construction
    """
    print("\n=== TABLE DASHBOARD ===")

    # Gain si on ameliore d'une classe (F->E, E->D, etc.)
    CLASSE_SUPERIEURE = {"G": "F", "F": "E", "E": "D", "D": "C", "C": "B", "B": "A"}

    rows = []
    for src, tgt in CLASSE_SUPERIEURE.items():
        gain_m2  = round(conso_map.get(src, 0) - conso_map.get(tgt, 0), 1)
        rows.append({
            "saut":          f"{src} → {tgt}",
            "classe_origine": src,
            "classe_cible":   tgt,
            "gain_kWh_m2":   gain_m2,
            "gain_pct":      round(gain_m2 / conso_map.get(src, 1) * 100, 1) if conso_map.get(src) else 0,
        })

    from pyspark.sql import Row
    df_dashboard = df.sparkSession.createDataFrame([Row(**r) for r in rows])
    df_dashboard.show()
    df_dashboard.write.mode("overwrite").parquet(GOLD_PATH + "dashboard_gains/")
    print("Table dashboard sauvegardee dans gold/dashboard_gains/")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Lecture Silver depuis {SILVER_PATH}...")
    df = spark.read.parquet(SILVER_PATH)
    print(f"Lignes lues : {df.count()}")

    # Analyses
    stats     = compute_gain_table(df)
    conso_map = compute_gain_matrix(df)

    # ML
    model, rmse, r2 = train_model(df)

    # Table dashboard
    build_dashboard_table(df, conso_map)

    print("\n=== RESUME GOLD ===")
    print(f"  RMSE modele : {rmse:.2f} kWh/m2/an")
    print(f"  R2 modele   : {r2:.4f}")
    print(f"  Resultats   : MinIO datalake/gold/")
    print(f"  Console     : http://localhost:9001")

    spark.stop()

if __name__ == "__main__":
    main()