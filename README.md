# 🏢 Smart Building Data Lake

> Data Lake pour la détection d'anomalies énergétiques et environnementales dans un bâtiment connecté  
> Cours *Data Lakes & Data Integration* — EFREI 2025-2026


---

## 📋 Description du projet

Ce projet implémente un **Data Lake de bout en bout** appliqué à un cas d'usage de **bâtiment connecté (smart building)**. Il centralise, transforme et expose des données hétérogènes issues de deux sources :

- **Source 1 — Dataset IoT (fichier batch)** : mesures historiques de capteurs déployés dans le bâtiment M5 de l'University of Sharjah (température, humidité, consommation électrique, occupation), couvrant la période janvier–juin 2024.
- **Source 2 — API qualité de l'air (Open-Meteo)** : données horaires de qualité de l'air extérieur (PM2.5, CO, indice AQI européen, et éventuellement CO2 selon l’endpoint retenu), ingérées périodiquement via un scheduler Airflow.

L'objectif principal est de **détecter des anomalies énergétiques et environnementales** dans le comportement du bâtiment, en croisant les signaux intérieurs (capteurs IoT) et extérieurs (qualité de l’air), à l’aide d’un modèle de Machine Learning non supervisé de type **Isolation Forest**.


---

## 🗂️ Architecture

```text
+-----------------------------+           +-------------------------------+
|   Source 1 : Dataset IoT    |           |   Source 2 : API Open-Meteo   |
|   (CSV batch – Zenodo)      |           |   Air Quality (JSON horaire)  |
+-------------+--------------+           +---------------+--------------+
              |  Ingestion batch (one-shot)             |  Scheduling Airflow (horaire)
              +------------------+----------------------+
                                 |
                                 v
                     +-------------------------+
                     |    Apache Airflow DAG   |
                     |   Orchestration globale |
                     +----------+--------------+
                                |
                                v
+-----------------------------------------------------------------------+
|                          ZONE RAW (S3)                                |
|  - Fichiers CSV bruts par type de capteur                             |
|  - Payloads JSON bruts de l'API qualité de l'air                      |
|  - Conservation des données telles que reçues, horodatées             |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                      ZONE STAGING (BigQuery)                          |
|  - stg_energy      : mesures électriques consolidées par appareil     |
|  - stg_environment : température et humidité par salle                |
|  - stg_occupancy   : présence et état des portes par salle            |
|  - stg_air_quality : qualité air extérieur (PM2.5, CO, AQI, etc.)     |
|  Nettoyage, typage, harmonisation timezone, jointures                 |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                      ZONE CURATED (BigQuery)                          |
|  - cur_building_measures : vue consolidée toutes sources              |
|  - cur_anomaly_scores    : scores d'anomalies Isolation Forest        |
|  - cur_daily_summary     : agrégats journaliers par salle/appareil    |
|  Enrichissement analytique + scoring d’anomalies sur données propres  |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                        API GATEWAY (FastAPI)                          |
|  GET /raw       → données brutes depuis S3                            |
|  GET /staging   → tables Staging BigQuery                             |
|  GET /curated   → tables Curated BigQuery                             |
|  GET /health    → état des services (Airflow, S3, BigQuery, API)      |
|  GET /stats     → métriques de remplissage des zones                  |
+-----------------------------------------------------------------------+
```


---

## 📦 Sources de données

### Source 1 — Dataset IoT Smart Building (Zenodo)

| Propriété | Valeur |
|---|---|
| **Nom** | Dataset of IoT-Based Energy and Environmental Parameters in a Smart Building Infrastructure |
| **Auteurs** | Oulefki, A., Abbes, A., Fatih, K., Bassel, S. (University of Sharjah) |
| **DOI** | `10.5281/zenodo.12750891` |
| **Licence** | Creative Commons Attribution 4.0 International (CC-BY 4.0) |
| **Format** | ZIP contenant 50+ fichiers CSV organisés en 8 dossiers |
| **Période** | 1er janvier → 20 juin 2024 |
| **URL de téléchargement** | https://zenodo.org/records/12750891/files/Dataset-of-IoT-Based-Energy-and-Environmental-Parameters-in-a-Smart-Building-Infrastructure-2.0.zip?download=1 |

**Structure des fichiers CSV :**

| Dossier | Appareils / Capteurs | Mesure |
|---|---|---|
| `Watt W/` | Coffee Machine, Desktop, Fridge, Kettle, Microwave, Printer, Water Dispenser | Puissance instantanée (W) |
| `KWh/Today/` | Coffee Machine, Desktop, Fridge, Kettle, Microwave, Printer, Water Dispenser | Consommation journalière (kWh) |
| `KWh/Total/` | Coffee Machine, Desktop, Fridge, Kettle, Microwave, Printer, Water Dispenser | Consommation cumulée (kWh) |
| `Apparent power VA/` | id. | Puissance apparente (VA) |
| `Voltage V/` | id. | Tension (V) |
| `Current A/` | id. | Courant (A) |
| `Temperature/` | Lab, Kitchen, Mailroom | Température intérieure (°C) |
| `Humidity/` | Lab, Kitchen, Mailroom | Humidité intérieure (%) |
| `Motion/` | Lab, Lab door, Kitchen, Mailroom | Présence (PIR) + état porte |


---

### Source 2 — Open-Meteo Air Quality API

| Propriété | Valeur |
|---|---|
| **Nom** | Open-Meteo Air Quality API |
| **URL de base** | `https://air-quality-api.open-meteo.com/v1/air-quality` |
| **Licence** | Gratuite, aucune clé API requise |
| **Format** | JSON |
| **Fréquence** | Horaire |
| **Coordonnées** | `latitude=25.3488`, `longitude=55.4054` (campus University of Sharjah) |
| **Variables** | `time`, `pm2_5` (μg/m³), `carbon_monoxide` (μg/m³), `european_aqi` (EAQI) (+ éventuellement `carbon_dioxide` si activé) |

**URL de récupération historique (période dataset) :**

```text
https://air-quality-api.open-meteo.com/v1/air-quality?latitude=25.3488&longitude=55.4054&hourly=pm2_5,carbon_monoxide,european_aqi&start_date=2024-01-01&end_date=2024-06-20&timezone=Asia%2FDubai
```

**URL pour le scheduling Airflow (dernières 24h) :**

```text
https://air-quality-api.open-meteo.com/v1/air-quality?latitude=25.3488&longitude=55.4054&hourly=pm2_5,carbon_monoxide,european_aqi&past_days=1&timezone=Asia%2FDubai
```

### Mode d’ingestion de l’API

Les données Open-Meteo sont ingérées en **batch planifié** via **Apache Airflow**. À chaque exécution, une tâche `ingest_api` appelle l’API, stocke la réponse JSON brute dans la **zone Raw** sur S3, puis une transformation Raw → Staging extrait les champs utiles (`time`, `pm2_5`, `carbon_monoxide`, `european_aqi`) pour alimenter `stg_air_quality` dans BigQuery. Les données sont ensuite intégrées dans `cur_building_measures` et utilisées par le modèle **Isolation Forest** en zone Curated.

---

## 🛠️ Stack technique

| Composant | Technologie |
|---|---|
| Zone Raw | AWS S3 (ou compatible S3 — MinIO en local) |
| Zone Staging | BigQuery |
| Zone Curated | BigQuery |
| Orchestration | Apache Airflow |
| Modèle ML principal | Isolation Forest (scikit-learn) |
| API Gateway | FastAPI |
| Langage | Python 3.11+ |


---

## 🧱 Schémas des tables principales

### Staging

**`stg_energy`** — mesures électriques consolidées par appareil :

```sql
CREATE TABLE stg_energy (
  timestamp          TIMESTAMP,
  device_id          STRING,
  room_id            STRING,
  watt_w             FLOAT64,
  apparent_power_va  FLOAT64,
  voltage_v          FLOAT64,
  current_a          FLOAT64,
  kwh_today          FLOAT64,
  kwh_total          FLOAT64
);
```

**`stg_environment`** — environnement intérieur :

```sql
CREATE TABLE stg_environment (
  timestamp      TIMESTAMP,
  room_id        STRING,
  temperature_c  FLOAT64,
  humidity_pct   FLOAT64
);
```

**`stg_occupancy`** — occupation :

```sql
CREATE TABLE stg_occupancy (
  timestamp       TIMESTAMP,
  room_id         STRING,
  motion_detected BOOL,
  door_open       BOOL
);
```

**`stg_air_quality`** — air extérieur (API) :

```sql
CREATE TABLE stg_air_quality (
  timestamp        TIMESTAMP,
  latitude         FLOAT64,
  longitude        FLOAT64,
  pm2_5            FLOAT64,
  carbon_monoxide  FLOAT64,
  european_aqi     FLOAT64
);
```


### Curated

**`cur_building_measures`** — vue consolidée pour le ML :

```sql
CREATE TABLE cur_building_measures (
  timestamp          TIMESTAMP,
  room_id            STRING,
  device_id          STRING,

  watt_w             FLOAT64,
  apparent_power_va  FLOAT64,
  voltage_v          FLOAT64,
  current_a          FLOAT64,
  kwh_today          FLOAT64,
  kwh_total          FLOAT64,

  temperature_c      FLOAT64,
  humidity_pct       FLOAT64,
  motion_detected    BOOL,
  door_open          BOOL,

  pm2_5              FLOAT64,
  carbon_monoxide    FLOAT64,
  european_aqi       FLOAT64
);
```

**`cur_anomaly_scores`** — résultats de détection d’anomalies :

```sql
CREATE TABLE cur_anomaly_scores (
  timestamp        TIMESTAMP,
  room_id          STRING,
  device_id        STRING,

  -- Features utilisées pour le scoring
  watt_w           FLOAT64,
  temperature_c    FLOAT64,
  humidity_pct     FLOAT64,
  pm2_5            FLOAT64,
  carbon_monoxide  FLOAT64,
  european_aqi     FLOAT64,

  -- Isolation Forest
  if_score         FLOAT64,
  if_is_anomaly    BOOL
);
```


---

## 🤖 Modèle de détection d'anomalies

La détection d'anomalies est au cœur du projet : l'objectif est d'identifier des comportements énergétiques ou environnementaux atypiques du bâtiment en croisant les données des capteurs IoT et les conditions extérieures.

### Isolation Forest

Isolation Forest est un modèle de Machine Learning non supervisé spécifiquement adapté à la détection d'anomalies dans des données tabulaires multivariées. Il est entraîné sur la table `cur_building_measures` afin d’apprendre les comportements “normaux” du bâtiment à partir de l’historique disponible.

Les principales features utilisées pour le scoring sont :

- `watt_w`
- `temperature_c`
- `humidity_pct`
- `pm2_5`
- `carbon_monoxide`
- `european_aqi`

Pour chaque observation, le modèle produit :

- un score continu (`if_score`),  
- un indicateur booléen (`if_is_anomaly`) obtenu via un seuil défini sur les données d’entraînement.

Les résultats sont centralisés dans la table `cur_anomaly_scores`, puis exposés via l’API (`GET /curated`) afin de permettre l’analyse ou la visualisation des événements suspects sans accéder directement au Data Lake.

> **Note :** une expérimentation complémentaire avec d’autres algorithmes (ex. DBSCAN ou autoencoder) pourra être menée à titre comparatif, mais le modèle principal du pipeline reste Isolation Forest.


---

## 🔌 API Gateway — Endpoints

| Endpoint | Méthode | Description |
|---|---|---|
| `/raw` | GET | Accès aux données brutes (S3) |
| `/staging` | GET | Accès aux tables Staging BigQuery |
| `/curated` | GET | Accès aux tables Curated (dont `cur_anomaly_scores`) |
| `/health` | GET | Vérifie l'état des services (Airflow, S3, BigQuery, API) |
| `/stats` | GET | Statistiques de remplissage des zones |


---

## 🚀 Installation et lancement

### Prérequis

- Python 3.11+
- Docker & Docker Compose
- Compte AWS (ou MinIO)
- Compte Google Cloud (BigQuery)

### 1. Cloner le dépôt

```bash
git clone https://github.com/<votre-username>/smart-building-datalake.git
cd smart-building-datalake
```

### 2. Configurer les variables d'environnement

```bash
cp .env.example .env
# Renseigner : AWS credentials, GCP project ID, BigQuery dataset, etc.
```

### 3. Lancer les services

```bash
docker-compose up -d
```

### 4. Accéder à Airflow

- URL : `http://localhost:8080`
- Login / Password : `airflow` / `airflow`

### 5. Déclencher le DAG d'ingestion

- Activer le DAG `smart_building_ingestion_dag` dans l'interface Airflow.
- Déclencher manuellement le premier run (ingestion batch du dataset IoT).
- Laisser ensuite le scheduler gérer l'ingestion horaire de l'API qualité de l'air.

### 6. Accéder à l'API Gateway

```bash
# L'API est disponible sur :
http://localhost:8000

# Exemples d'appels :
curl http://localhost:8000/health
curl http://localhost:8000/stats
curl "http://localhost:8000/curated?is_anomaly=true&start_date=2024-01-01&end_date=2024-06-20"
```


---

## 📁 Structure du dépôt (prévue)

```text
smart-building-datalake/
├── dags/
│   └── smart_building_dag.py        # DAG Airflow principal
├── ingestion/
│   ├── ingest_dataset.py            # Téléchargement et dépôt Zenodo → S3
│   └── ingest_api.py                # Appel Open-Meteo → S3
├── transformation/
│   ├── raw_to_staging.py            # S3 → BigQuery Staging
│   └── staging_to_curated.py        # Staging → Curated + ML
├── ml/
│   ├── train_isolation_forest.py    # Entraînement Isolation Forest
│   └── apply_models.py              # Scoring des nouvelles données
├── api/
│   └── main.py                      # FastAPI — API Gateway
├── docker-compose.yml
├── .env.example
└── README.md
```
l

---

## 📊 Livrables

- [x] Architecture documentée (zones Raw / Staging / Curated)
- [x] Deux sources de données (dataset fichier + API)
- [x] Pipeline d'intégration Apache Airflow
- [x] Modèle ML de détection d'anomalies (Isolation Forest)
- [x] API Gateway (FastAPI) avec les endpoints requis
- [x] README exhaustif
- [ ] Dépôt GitHub finalisé avec l'intégralité du code source


---

## 📚 Références

- Oulefki, A., Abbes, A., Fatih, K., & Bassel, S. (2024). *Dataset of IoT-Based Energy and Environmental Parameters in a Smart Building Infrastructure* (v2.0) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.12750891
- Open-Meteo. *Air Quality API*. https://open-meteo.com/en/docs/air-quality-api


---

*Projet réalisé dans le cadre du cours Data Lakes & Data Integration — EFREI 2025-2026*