📌 Description détaillée des étapes
1. 🔹 Data Sources
Données issues de fichiers CSV :
DPE (Diagnostic de Performance Énergétique)
ENEDIS (consommation énergétique)
2. 🔹 Ingestion (Kafka Producer)
Script Python envoie les données vers Kafka
Utilisation de kafka-python
Envoi en streaming par batch (chunks)

📂 Script : Kafka/producer.py

3. 🔹 Message Broker (Kafka)
Kafka joue le rôle de système de streaming
Topics utilisés :
dpe_topic
enedis_topic
4. 🔹 Streaming Processing (Spark)
Spark Structured Streaming lit les données depuis Kafka
Conversion en format exploitable
Écriture dans le Data Lake (MinIO)

📂 Script : spark/kafka_to_bronze.py

5. 🥉 Bronze Layer (Data Lake)
Données brutes stockées sans transformation
Format : Parquet
Stockage : MinIO (S3 compatible)
s3a://data-lake/bronze/
6. 🥈 Silver Layer (Transformation)
Nettoyage des données
Suppression des valeurs nulles
Typage des colonnes
Jointure entre datasets DPE et ENEDIS
Calculs :
consommation par logement
consommation par m²

📂 Script : spark/bronze_to_silver.py

7. 🥇 Gold Layer (Analyse)
Agrégation des données
Calcul des indicateurs :
consommation moyenne
nombre de logements
Analyse par :
étiquette DPE
type de bâtiment
période de construction

📂 Script : spark/silver_to_gold.py

8. 🤖 Machine Learning
Modèle de régression linéaire
Prédiction de consommation énergétique
Utilisation de Spark MLlib

📂 Script : spark/train_model.py

9. 📊 Dashboard & Visualisation
Préparation des données pour visualisation
Création d’un dashboard interactif avec Streamlit

📂 Scripts :

spark/prepare_dashboard.py
spark/dashboard_app.py
⚙️ Technologies utilisées
Apache Kafka → ingestion streaming
Apache Spark → traitement distribué
MinIO → stockage Data Lake
Python → scripting
Streamlit → visualisation
MLlib → machine learning
