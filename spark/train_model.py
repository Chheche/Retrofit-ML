from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("TrainModel") \
    .master("local[*]") \
    .getOrCreate()

print("Loading GOLD dataset...")

# -----------------------------
# LOAD GOLD DATA
# -----------------------------
df = spark.read.parquet("data_lake/gold/dpe_analysis")

df.show(5)

# -----------------------------
# PREPARE FEATURES
# -----------------------------
assembler = VectorAssembler(
    inputCols=["nb_logements"],
    outputCol="features"
)

data = assembler.transform(df)

# -----------------------------
# TRAIN MODEL
# -----------------------------
lr = LinearRegression(
    featuresCol="features",
    labelCol="conso_moyenne_kwh_logement"
)

model = lr.fit(data)

print("Model trained successfully!")

# -----------------------------
# SAVE MODEL
# -----------------------------
model.write().overwrite().save("data_lake/models/energy_model")

print("Model saved!")

spark.stop()