from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, lag
from pyspark.sql.window import Window

# -----------------------------
# Spark Session
# -----------------------------

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 8)

print("Loading Silver dataset")

# -----------------------------
# READ SILVER
# -----------------------------

df = spark.read.parquet("data_lake/silver/dpe_enedis")

print("Silver dataset loaded")

# -----------------------------
# GOLD DATASET
# -----------------------------

gold = df.groupBy(
    "etiquette_dpe",
    "type_batiment",
    "periode_construction"
).agg(
    avg("conso_par_logement").alias("conso_moyenne_kwh_logement"),
    avg("conso_kwh_m2").alias("conso_moyenne_kwh_m2"),
    count("*").alias("nb_logements")
)

print("Aggregation completed")

# -----------------------------
# GAIN ENERGETIQUE ENTRE DPE
# -----------------------------

window = Window.orderBy("etiquette_dpe")

gold_gain = gold.withColumn(
    "conso_classe_precedente",
    lag("conso_moyenne_kwh_logement").over(window)
)

gold_gain = gold_gain.withColumn(
    "gain_kwh",
    col("conso_classe_precedente") - col("conso_moyenne_kwh_logement")
)

# -----------------------------
# SAVE GOLD
# -----------------------------

gold_gain.write.mode("overwrite").parquet(
    "data_lake/gold/dpe_analysis"
)

print("Gold dataset created successfully")

# -----------------------------
# PREVIEW RESULT
# -----------------------------

gold_gain.show(20)

spark.stop()