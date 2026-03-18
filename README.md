#  Prédiction de la Consommation Électrique des Logements Français

##  Description du projet

Ce projet s'inscrit dans le cadre du challenge
[Diagnostics de Performance Énergétique](https://defis.data.gouv.fr/defis/diagnostics-de-performance-energetique).

**Objectif** : Construire un modèle de Machine Learning qui prédit
la consommation électrique d'un logement (en kWh/m²/an) à partir
de ses caractéristiques DPE, afin de quantifier les économies
potentielles liées à une rénovation énergétique.

**Cas d'usage concret** :
> "Si j'améliore mon DPE de E à C, combien vais-je économiser
> sur ma facture électrique ?"

---

##  Architecture complète du pipeline
```
Sources de données (data.gouv.fr)
           │
           ▼
┌──────────────────────┐
│   Kafka Producer     │  ← scripts/producer/
│  (3 topics Kafka)    │
│  - addresses         │
│  - dpe               │
│  - consommation      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Apache Kafka       │  ← Broker de messages
│   (streaming)        │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Kafka Consumer     │  ← scripts/consumer/
│   → MinIO Bronze     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────┐
│           MinIO (Data Lake)          │
│                                      │
│  bronze/  ← données brutes           │
│  ├── addresses_batch_*.json          │
│  ├── dpe_batch_*.json                │
│  └── consommation-annuelle-*.json    │
│                                      │
│  silver/  ← données nettoyées        │
│  ├── addresses/  (parquet)           │
│  ├── dpe/        (parquet)           │
│  └── consommation/ (parquet)         │
│                                      │
│  gold/    ← résultats ML             │
│  ├── predictions/ (parquet)          │
│  ├── metrics/     (parquet)          │
│  └── model/       (Spark ML)         │
└──────────┬───────────────────────────┘
           │
           ▼
┌──────────────────────┐
│   Apache Spark       │  ← scripts/processing/
│   Bronze → Silver    │  clean_all.py
│   Silver → Gold (ML) │  gold_ml.py
└──────────────────────┘
```

---

##  Sources des données

| Dataset | Source | Format | Description |
|---|---|---|---|
| DPE depuis 2021 | [data.gouv.fr](https://www.data.gouv.fr) | JSON | Diagnostics de Performance Énergétique |
| Consommation électrique | [data.gouv.fr](https://www.data.gouv.fr) | JSON | Consommation annuelle résidentielle par adresse |
| Base Adresse Nationale | [adresse.data.gouv.fr](https://adresse.data.gouv.fr) | JSON | Référentiel national des adresses |

---

## 🔧 Infrastructure

| Outil | Version | Rôle |
|---|---|---|
| **Apache Kafka** | 4.2.0 | Streaming et ingestion des données |
| **MinIO** | latest | Data Lake local compatible S3 |
| **Apache Spark** | 4.0.2 | Traitement distribué des données |
| **Docker** | - | Containerisation de tous les services |
| **Spark MLlib** | 4.0.2 | Machine Learning distribué |
| **Python** | 3.x | Scripts producteur/consommateur |

---

##  Structure du projet
```
dpe_project/
├── scripts/
│   ├── producer/                    # Producteurs Kafka
│   │   └── kafka_producer.py        # Envoie les données vers Kafka
│   ├── consumer/                    # Consommateurs Kafka
│   │   └── kafka_consumer.py        # Lit Kafka et stocke dans MinIO Bronze
│   └── processing/                  # Traitement Spark
│       ├── clean_all.py             # Bronze → Silver (nettoyage)
│       ├── fix_consommation.py      # Correction structure JSON consommation
│       ├── fix_dpe.py               # Correction structure JSON DPE
│       └── gold_ml.py               # Silver → Gold (ML)
└── README.md
```

---

##  Instructions d'exécution

### Prérequis

- Docker Desktop installé et en cours d'exécution
- JARs Hadoop dans un dossier `jars/` (non versionné) :
  - `hadoop-aws-3.3.4.jar`
  - `aws-java-sdk-bundle-1.12.262.jar`

### Étape 1 — Démarrer l'infrastructure
```bash
# MinIO
docker run -d -p 9000:9000 -p 9001:9001 \
  --name minio \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Kafka
docker run -d --name kafka -p 9092:9092 apache/kafka:4.2.0
```

### Étape 2 — Ingestion via Kafka
```bash
# 1. Lancer le producteur (envoie les données vers Kafka)
python scripts/producer/kafka_producer.py

# 2. Lancer le consommateur (lit Kafka et stocke dans MinIO Bronze)
python scripts/consumer/kafka_consumer.py
```

### Étape 3 — Nettoyage Silver (Spark)
```bash
docker run -it \
  -v "/chemin/vers/scripts/processing:/scripts" \
  -v "/chemin/vers/jars:/jars" \
  -e HOME=/root --user root apache/spark:4.0.2 \
  /bin/bash -c "pip install numpy --quiet && \
  /opt/spark/bin/spark-submit \
  --jars /jars/aws-java-sdk-bundle-1.12.262.jar,/jars/hadoop-aws-3.3.4.jar \
  /scripts/clean_all.py"
```

### Étape 4 — Machine Learning Gold (Spark)
```bash
docker run -it \
  -v "/chemin/vers/scripts/processing:/scripts" \
  -v "/chemin/vers/jars:/jars" \
  -e HOME=/root --user root apache/spark:4.0.2 \
  /bin/bash -c "pip install numpy --quiet && \
  /opt/spark/bin/spark-submit \
  --jars /jars/aws-java-sdk-bundle-1.12.262.jar,/jars/hadoop-aws-3.3.4.jar \
  /scripts/gold_ml.py"
```

---

##  Modèles de Machine Learning

### Objectif (DSO)
Prédire la consommation électrique en kWh/m²/an d'un logement
à partir de ses caractéristiques DPE.

### Variables explicatives utilisées

| Variable | Type | Description |
|---|---|---|
| `etiquette_dpe` | Catégorielle | Étiquette DPE (A à G) |
| `type_batiment` | Catégorielle | Maison / Appartement / Immeuble |
| `periode_construction` | Catégorielle | Époque de construction |
| `zone_climatique` | Catégorielle | Zone H1 / H2 / H3 |
| `qualite_isolation_enveloppe` | Catégorielle | Qualité isolation globale |
| `qualite_isolation_murs` | Catégorielle | Qualité isolation murs |
| `qualite_isolation_menuiseries` | Catégorielle | Qualité isolation fenêtres |
| `qualite_isolation_plancher_bas` | Catégorielle | Qualité isolation plancher bas |
| `qualite_isolation_plancher_haut_comble_perdu` | Catégorielle | Qualité isolation toiture |
| `type_energie_principale_chauffage` | Catégorielle | Gaz / Électricité / etc. |
| `type_installation_chauffage` | Catégorielle | Individuel / Collectif |
| `ubat_w_par_m2_k` | Numérique | Coefficient de déperdition thermique |
| `deperditions_enveloppe` | Numérique | Déperditions thermiques totales |
| `surface_habitable_immeuble` | Numérique | Surface en m² |
| `conso_moyenne_commune_mwh` | Numérique | Consommation moyenne de la commune |

### Résultats

| Modèle | RMSE (kWh/m²) | R² | Description |
|---|---|---|---|
| Random Forest | 30.31 | 0.8768 | 50 arbres, profondeur 8 |
| **GBT Gradient Boosting** | **28.82** | **0.8886** | 50 itérations, profondeur 8 |

** Meilleur modèle : GBT avec R² = 0.89**

Le modèle explique **89% de la variabilité** de la consommation
électrique. L'erreur moyenne est de **28.82 kWh/m²**,
soit ~19% d'erreur relative.

---

##  Dashboard & Déploiement

### Lancer le dashboard Streamlit
```bash
cd scripts/dashboard
pip install -r requirements.txt
python -m streamlit run app.py
```

Ouvre **http://localhost:8501**

### Fonctionnalités du dashboard

| Onglet | Description |
|---|---|
| **Dashboard** | KPIs, distribution DPE, zones climatiques, réel vs prédit, top communes |
| **Simulateur** | Calcul des économies et ROI d'une rénovation énergétique |
| **Modèle ML** | Comparaison RF vs GBT, importance des variables, pipeline complet |

##  Aperçu du dashboard

### Dashboard — Vue d'ensemble
![Dashboard DPE](assets/1a.png)

### Dashboard — Graphiques d'analyse
![Graphiques](assets/2a.png)

### Simulateur de rénovation énergétique
![Simulateur 1](assets/3a.png)

### Simulateur — Résultats et économies
![Simulateur 2](assets/4a.png)
