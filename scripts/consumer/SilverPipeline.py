from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_date

# Créer la session Spark
spark = SparkSession.builder \
    .appName("SilverPipeline") \
    .getOrCreate()

# Config MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://127.0.0.1:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Lire les CSV
addresses_df = spark.read.option("header", "true").csv(
    "s3a://datalake/bronze/adresses-france.csv"
)

consumption_df = spark.read.option("header", "true").csv(
    "s3a://datalake/bronze/consommation-annuelle-residentielle-par-adresse.csv"
)

# Schema
print("=== Schema Adresses ===")
addresses_df.printSchema()

print("=== Schema Consommation ===")
consumption_df.printSchema()

# Valeurs manquantes
print("=== Valeurs manquantes Adresses ===")
addresses_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in addresses_df.columns]).show()

print("=== Valeurs manquantes Consommation ===")
consumption_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in consumption_df.columns]).show()

# Statistiques
print("=== Statistiques Adresses ===")
addresses_df.describe().show()

print("=== Statistiques Consommation ===")
consumption_df.describe().show()

# Nettoyage
addresses_silver = addresses_df.dropDuplicates() \
    .withColumn("ingestion_date", current_date())

addresses_silver.write.mode("overwrite").parquet("s3a://datalake/silver/addresses/")

consumption_silver = consumption_df.dropDuplicates() \
    .withColumn("ingestion_date", current_date())

consumption_silver.write.mode("overwrite").parquet("s3a://datalake/silver/consumption/")

print("Silver pipeline terminé")