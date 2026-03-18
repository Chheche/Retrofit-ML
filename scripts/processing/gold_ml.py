from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper, lpad, avg
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("GoldML") \
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

# =============================================================
# 1. LECTURE DEPUIS SILVER
# =============================================================
print("\n========== LECTURE SILVER ==========")

df_dpe = spark.read.parquet("s3a://datalake/silver/dpe/")

# Consommation — lire parquet silver et gérer les 2 cas possibles
df_conso_raw = spark.read.parquet("s3a://datalake/silver/consommation/")

if 'data' in df_conso_raw.columns:
    df_conso = df_conso_raw.select("data.*")
else:
    df_conso = df_conso_raw

print(f"DPE   : {df_dpe.count()} lignes, {len(df_dpe.columns)} colonnes")
print(f"Conso : {df_conso.count()} lignes, {len(df_conso.columns)} colonnes")
print(f"Colonnes Conso : {df_conso.columns}")

# =============================================================
# 2. PREPARATION CONSOMMATION
# =============================================================
print("\n========== PREPARATION CONSOMMATION ==========")

df_conso = df_conso \
    .withColumnRenamed("Code Commune", "code_insee_join") \
    .withColumnRenamed(
        "consommation_annuelle_moyenne_par_site_de_ladresse_mwh",
        "conso_reelle_mwh"
    )

df_conso = df_conso.withColumn(
    "code_insee_join",
    lpad(col("code_insee_join").cast("string"), 5, "0")
)

df_conso_agg = df_conso.groupBy("code_insee_join").agg(
    avg(col("conso_reelle_mwh").cast("double")).alias("conso_moyenne_commune_mwh")
)

print(f"Communes uniques : {df_conso_agg.count()}")

# =============================================================
# 3. PREPARATION DPE
# =============================================================
print("\n========== PREPARATION DPE ==========")

cat_cols = [
    "etiquette_dpe",
    "type_batiment",
    "periode_construction",
    "zone_climatique",
    "qualite_isolation_enveloppe",
    "qualite_isolation_murs",
    "qualite_isolation_menuiseries",
    "qualite_isolation_plancher_bas",
    "qualite_isolation_plancher_haut_comble_perdu",
    "type_energie_principale_chauffage",
    "type_installation_chauffage"
]

num_cols = [
    "ubat_w_par_m2_k",
    "deperditions_enveloppe",
    "surface_habitable_immeuble"
]

target = "conso_5_usages_par_m2_ef"

cols_dpe = ["code_insee_ban"] + cat_cols + num_cols + [target]
df_dpe = df_dpe.select([c for c in cols_dpe if c in df_dpe.columns])

df_dpe = df_dpe.withColumn(
    "code_insee_ban",
    lpad(col("code_insee_ban").cast("string"), 5, "0")
)

# =============================================================
# 4. JOINTURE
# =============================================================
print("\n========== JOINTURE ==========")

df = df_dpe.join(
    df_conso_agg,
    df_dpe["code_insee_ban"] == df_conso_agg["code_insee_join"],
    how="left"
).drop("code_insee_join")

print(f"Après jointure : {df.count()} lignes")

# =============================================================
# 5. NETTOYAGE ML
# =============================================================
print("\n========== NETTOYAGE ML ==========")

df = df.filter(col(target).isNotNull())
df = df.filter(col(target) > 0)
df = df.filter(col(target) < 2000)

df = df.withColumn("etiquette_dpe", upper(trim(col("etiquette_dpe"))))

for c in cat_cols:
    if c in df.columns:
        df = df.withColumn(c, when(col(c).isNull(), "Inconnu").otherwise(col(c)))

for c in num_cols + ["conso_moyenne_commune_mwh"]:
    if c in df.columns:
        df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c).cast("double")))

print(f"Lignes prêtes pour ML : {df.count()}")

# =============================================================
# 6. FEATURE ENGINEERING
# =============================================================
print("\n========== FEATURE ENGINEERING ==========")

indexers = []
indexed_cols = []

for c in cat_cols:
    if c in df.columns:
        out = c + "_idx"
        indexers.append(StringIndexer(inputCol=c, outputCol=out, handleInvalid="keep"))
        indexed_cols.append(out)

for c in num_cols + ["conso_moyenne_commune_mwh"]:
    if c in df.columns:
        indexed_cols.append(c)

assembler = VectorAssembler(
    inputCols=indexed_cols,
    outputCol="features",
    handleInvalid="keep"
)

# =============================================================
# 7. SPLIT TRAIN / TEST
# =============================================================
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Train : {train_df.count()} lignes")
print(f"Test  : {test_df.count()} lignes")

# =============================================================
# 8. RANDOM FOREST
# =============================================================
print("\n========== MODELE 1 : RANDOM FOREST ==========")

rf = RandomForestRegressor(featuresCol="features", labelCol=target, numTrees=50, maxDepth=8, seed=42)
pipeline_rf = Pipeline(stages=indexers + [assembler, rf])
model_rf = pipeline_rf.fit(train_df)
pred_rf = model_rf.transform(test_df)

evaluator_rmse = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="rmse")
evaluator_r2   = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="r2")

rmse_rf = evaluator_rmse.evaluate(pred_rf)
r2_rf   = evaluator_r2.evaluate(pred_rf)
print(f"Random Forest → RMSE : {rmse_rf:.2f} | R² : {r2_rf:.4f}")

# =============================================================
# 9. GBT
# =============================================================
print("\n========== MODELE 2 : GRADIENT BOOSTING ==========")

gbt = GBTRegressor(featuresCol="features", labelCol=target, maxIter=50, maxDepth=8, seed=42)
pipeline_gbt = Pipeline(stages=indexers + [assembler, gbt])
model_gbt = pipeline_gbt.fit(train_df)
pred_gbt = model_gbt.transform(test_df)

rmse_gbt = evaluator_rmse.evaluate(pred_gbt)
r2_gbt   = evaluator_r2.evaluate(pred_gbt)
print(f"GBT           → RMSE : {rmse_gbt:.2f} | R² : {r2_gbt:.4f}")

# =============================================================
# 10. COMPARAISON
# =============================================================
print("\n========== COMPARAISON ==========")
print(f"Random Forest → RMSE : {rmse_rf:.2f} | R² : {r2_rf:.4f}")
print(f"GBT           → RMSE : {rmse_gbt:.2f} | R² : {r2_gbt:.4f}")

if rmse_gbt < rmse_rf:
    print("\n🏆 Meilleur modèle : GBT")
    best_model = model_gbt
    best_predictions = pred_gbt
    best_name = "GBT"
    best_rmse = rmse_gbt
    best_r2 = r2_gbt
else:
    print("\n🏆 Meilleur modèle : Random Forest")
    best_model = model_rf
    best_predictions = pred_rf
    best_name = "RandomForest"
    best_rmse = rmse_rf
    best_r2 = r2_rf

# =============================================================
# 11. SAUVEGARDE GOLD
# =============================================================
print("\n========== SAUVEGARDE GOLD ==========")

best_predictions.select(
    "code_insee_ban", "etiquette_dpe", "type_batiment",
    "surface_habitable_immeuble", target, "prediction"
).write.mode("overwrite").parquet("s3a://datalake/gold/predictions/")

spark.createDataFrame([
    ("RandomForest", "RMSE", float(rmse_rf)),
    ("RandomForest", "R2",   float(r2_rf)),
    ("GBT",          "RMSE", float(rmse_gbt)),
    ("GBT",          "R2",   float(r2_gbt)),
], ["model", "metric", "value"]) \
    .write.mode("overwrite").parquet("s3a://datalake/gold/metrics/")

best_model.write().overwrite().save("s3a://datalake/gold/model/")

print(f"✅ Prédictions  → gold/predictions/")
print(f"✅ Métriques    → gold/metrics/")
print(f"✅ Modèle ({best_name}) → gold/model/")
print(f"\n📊 Résultats : {best_name} | RMSE : {best_rmse:.2f} | R² : {best_r2:.4f}")

spark.stop()
print("\n🎉 Étape Gold terminée !")