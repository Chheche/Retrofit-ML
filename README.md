## Retrofit-ML

# Pipeline DPE — Diagnostics de Performance Énergétique

Défi data.gouv.fr — Étape 1 : Ingestion des données (couche Bronze)

## Source des données

- **Dataset** : DPE v2 — Logements existants
- **Producteur** : ADEME (Agence de la transition écologique)
- **URL** : https://data.ademe.fr/datasets/dpe-v2-logements-existants
- **Licence** : Licence Ouverte / Open Licence 2.0
- **Description** : Diagnostics de Performance Énergétique réalisés sur des logements existants en France. Contient les informations sur la consommation énergétique, les émissions de GES, le type de chauffage, etc.

---

## Architecture du pipeline

```
ADEME API
    │
    ▼
Producer Python  ──► Kafka (topic: open-data)  ──► Consumer Python  ──► MinIO

MinIO
 │
 └── datalake/
     └── bronze/
        └── date=YYYY-MM-DD/
             └── dpe_*.json
```

---

## Démarrage rapide

### Prérequis

- Docker & Docker Compose installés
- Python 3.9+
- Git

### Étape 1 — Lancer l'infrastructure (Personne 1)

```bash
cd infra/
docker compose up -d

# Vérifier que tout est up
docker compose ps
```

**Services lancés :**
| Service | URL | Utilité |
|---------|-----|---------|
| Kafka | `localhost:9092` | Broker de messages |
| Kafka UI | http://localhost:8080 | Interface web Kafka |
| MinIO API | `localhost:9000` | API S3-compatible |
| MinIO Console | http://localhost:9001 | Interface web MinIO |

**Credentials MinIO :** `minioadmin` / `minioadmin`

### Étape 2 — Créer le topic Kafka (Personne 1)

```bash
cd infra/
pip install kafka-python
python create_topic.py
```

### Étape 3 — Lancer le Consumer en premier (Personne 3)

> Lancer le consumer **avant** le producer pour ne rien manquer.

```bash
cd consumer/
pip install -r requirements.txt

# Lancement standard
python consumer.py

# Options avancées
python consumer.py --batch-size 200 --flush-interval 20
```

### Étape 4 — Lancer le Producer (Personne 2)

```bash
cd producer/
pip install -r requirements.txt

# Test avec 1000 lignes
python producer.py --limit 1000

# Production complète (5000 lignes par défaut)
python producer.py
```

### Étape 5 — Vérifier la couche Bronze (Personne 3)

```bash
cd consumer/
python verify_bronze.py
```

Et visuellement sur : http://localhost:9001 → Bucket `datalake` → Dossier `bronze/`

---

## Structure des fichiers dans MinIO

```
datalake/
└── bronze/
    └── date=2024-01-15/
        ├── dpe_143021_0000.json    ← batch 1 (500 records)
        ├── dpe_143051_0001.json    ← batch 2 (500 records)
        └── dpe_143121_0002.json    ← batch 3 (500 records)
```

Chaque fichier est au format **NDJSON** (un JSON par ligne).

---

## Format d'un message DPE

```json
{
  "N°DPE": "2193E0971280J",
  "Date_réception_DPE": "2021-07-01",
  "Etiquette_DPE": "D",
  "Etiquette_GES": "D",
  "Conso_5_usages_é_finale": 152.3,
  "Emission_GES_5_usages_par_m²": 22.1,
  "Surface_habitable_logement": 75.0,
  "Type_bâtiment": "Appartement",
  "_pipeline_source": "ademe-dpe-v2",
  "_pipeline_timestamp": "2024-01-15T14:30:21Z"
}
```

---

## Arrêter l'infrastructure

```bash
cd infra/
docker compose down      # arrêter sans supprimer les données
docker compose down -v   # arrêter ET supprimer les données MinIO
```


