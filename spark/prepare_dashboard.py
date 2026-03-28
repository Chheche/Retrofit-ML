from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("DashboardPrep") \
    .master("local[*]") \
    .getOrCreate()

print("Loading GOLD dataset...")

# -----------------------------
# LOAD GOLD DATA
# -----------------------------
df = spark.read.parquet("data_lake/gold/dpe_analysis")

# -----------------------------
# DASHBOARD DATA
# -----------------------------

# Moyenne conso par DPE
dashboard = df.groupBy("etiquette_dpe").agg(
    avg("conso_moyenne_kwh_logement").alias("conso_moyenne")
)

dashboard.show()

# -----------------------------
# SAVE FOR DASHBOARD
# -----------------------------
dashboard.write.mode("overwrite").csv(
    "data_lake/dashboard/dpe",
    header=True
)

print("Dashboard data ready!")

spark.stop()