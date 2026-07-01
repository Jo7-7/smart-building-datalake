# Smart Building Data Lake

Pipeline de données complet pour la détection d'anomalies énergétiques dans un
bâtiment connecté. Le projet implémente une architecture médaillon
**Raw → Staging → Curated**, alimentée par deux sources (un dataset de fichiers
et une API), orchestrée par Apache Airflow, enrichie par un modèle de détection
d'anomalies (Isolation Forest), et exposée via une API Gateway FastAPI.

> Projet du cours *Data Lakes & Data Integration* — EFREI 2025-2026 (Yvann Vincent).

---

## 1. Contexte et objectif

Le projet exploite les données du bâtiment M5 de l'University of Sharjah
(capteurs IoT exportés depuis Chronograf/InfluxDB, janvier à juin 2024),
complétées par des données de qualité de l'air extérieur récupérées via l'API
Open-Meteo.

L'objectif est de consolider ces signaux hétérogènes dans un Data Lake
structuré, jusqu'à produire des tables analytiques, puis de **détecter les
anomalies de consommation par appareil** à l'aide d'un modèle non supervisé.

---

## 2. Architecture

```text
+-----------------------------+           +-------------------------------+
|   Source 1 : Dataset IoT    |           |   Source 2 : API Open-Meteo   |
|   (53 CSV batch - Zenodo)   |           |   Air Quality (JSON horaire)  |
+-------------+---------------+           +---------------+---------------+
              |  Ingestion batch                         |  Scheduling Airflow
              +---------------------+--------------------+   (horaire)
                                    |
                                    v
                        +-----------------------+
                        |   Apache Airflow      |
                        |  Orchestration (3 DAG)|
                        +-----------+-----------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                        ZONE RAW (MinIO / S3)                          |
|  - source_dataset/...  : CSV bruts des capteurs IoT                   |
|  - source_api/...      : payloads JSON bruts de l'API                 |
|  - models/...          : modeles Isolation Forest serialises         |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                      ZONE STAGING (BigQuery)                          |
|  stg_sensor_timeseries : table longue unifiee                        |
|  (event_time, metric_family, metric_value, entity_name, ...)         |
|  Nettoyage, typage, harmonisation timezone, format long              |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                      ZONE CURATED (BigQuery)                          |
|  cur_energy_by_device     : conso electrique pivotee par appareil    |
|  cur_environment_by_room  : temperature/humidite/presence par piece  |
|  cur_daily_summary        : agregats journaliers + qualite d'air     |
|  cur_anomaly_scores       : scores d'anomalies Isolation Forest      |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                        API GATEWAY (FastAPI)                          |
|  GET /raw       -> inventaire des objets de la zone Raw (MinIO)       |
|  GET /staging   -> lignes de la table Staging BigQuery               |
|  GET /curated   -> lignes des tables Curated (dont anomaly_scores)   |
|  GET /health    -> etat des services (MinIO, BigQuery)               |
|  GET /stats     -> metriques de remplissage des zones                |
+-----------------------------------------------------------------------+
```

| Zone | Technologie | Rôle |
|------|-------------|------|
| **Raw** | MinIO (compatible S3) | Données brutes telles qu'ingérées + modèles ML |
| **Staging** | BigQuery | Données normalisées en format long unifié |
| **Curated** | BigQuery | Tables analytiques pivotées, agrégées et scorées |

---

## 3. Choix techniques et justifications

**MinIO pour la zone Raw.** Le cahier des charges impose Elasticsearch ou un
stockage S3 pour la zone Raw. MinIO est une implémentation S3 open-source qui
tourne en local via Docker, ce qui satisfait l'exigence tout en restant léger et
reproductible. Les objets sont organisés par préfixes (`source_dataset/`,
`source_api/`, `models/`) pour séparer sources et artefacts.

**BigQuery pour Staging et Curated.** Le sujet laisse libre le choix des
technologies pour ces zones, sous réserve de justification. BigQuery gère
nativement de gros volumes de séries temporelles sans administration, son SQL
est idéal pour les pivots et agrégations entre Staging et Curated, et il découple
le stockage analytique du stockage objet — une séparation Data Lake / Data
Warehouse cohérente avec les architectures modernes.

**Format long en Staging.** Les sources ont des granularités différentes
(consommation par appareil, environnement par pièce, air par site). Plutôt que
de forcer un schéma large hétérogène, la Staging utilise une table unique au
format long, ce qui absorbe ces granularités. Le pivot vers un format large
adapté à chaque grain est fait en Curated.

**Airflow pour l'orchestration.** Choisi plutôt que DVC car il gère nativement
le scheduling de l'API, exigence explicite du sujet, et correspond au pattern
`extract >> transform >> load` du cours.

---

## 4. Structure du dépôt

```text
smart-building-datalake/
├── ingestion/
│   ├── ingest_dataset.py      # Fichiers IoT  -> Raw
│   └── ingest_api.py          # API Open-Meteo -> Raw
├── transformation/
│   ├── raw_to_staging.py      # Raw (CSV IoT)  -> Staging
│   ├── api_to_staging.py      # Raw (JSON API) -> Staging
│   └── staging_to_curated.py  # Staging -> 3 tables Curated
├── ml/
│   ├── train_isolation_forest.py  # Entrainement (1 modele par appareil)
│   └── apply_models.py            # Scoring -> cur_anomaly_scores
├── api/
│   └── main.py                # API Gateway FastAPI (5 endpoints)
├── dags/
│   └── smart_building_dag.py  # 3 DAG Airflow
├── data/                      # Dataset IoT (53 CSV)
├── docker-compose.yml         # Stack complète
├── Dockerfile                 # Image pipeline
├── Dockerfile.airflow         # Image Airflow + Docker CLI
├── requirements.txt
└── .env                       # Variables (non versionné)
```

---

## 5. Prérequis

- Docker et Docker Compose
- Un projet Google Cloud avec BigQuery activé
- Une clé de compte de service GCP, dans `credentials/gcp-service-account.json`
- Deux datasets BigQuery créés au préalable : `staging` et `curated`

Fichier `.env` à la racine (non versionné) :

```env
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
GCP_PROJECT_ID=smart-building-datalake
GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-service-account.json
```

---

## 6. Build et démarrage

```bash
docker compose build          # construire les images
docker compose up -d          # démarrer la stack complète
docker compose ps             # vérifier que tout tourne
```

Le service `minio-init` crée automatiquement le bucket `raw` au démarrage, et la
zone Raw persiste sur disque via `./minio-data`.

Interfaces : Airflow sur **http://localhost:8080** (`airflow` / `airflow`), API
sur **http://localhost:8000** (docs sur `/docs`), console MinIO sur
**http://localhost:9001**.

---

## 7. Exécution du pipeline

### Via Airflow (recommandé)

Sur http://localhost:8080, activer puis déclencher le DAG
`smart_building_full_pipeline`.

> **Ordre sur environnement vierge.** Le pipeline complet fait de l'inférence
> (il applique des modèles existants). Sur une installation neuve, lancer une
> première fois le DAG `smart_building_train_models` **avant** le pipeline
> complet, afin que les modèles existent dans MinIO.

### Manuellement (script par script)

```bash
# 1. Ingestion IoT -> Raw
docker compose run --rm --no-deps pipeline ingestion/ingest_dataset.py \
  --dataset-root data --bucket raw --source-name smart_building_dataset \
  --minio-endpoint minio:9000 --minio-access-key minio --minio-secret-key minio123

# 2. Ingestion API -> Raw
docker compose run --rm --no-deps pipeline ingestion/ingest_api.py \
  --bucket raw --source-name open_meteo_api --api-type air_quality \
  --latitude 25.3488 --longitude 55.4054 \
  --start-date 2024-01-01 --end-date 2024-06-20 --timezone GMT \
  --minio-endpoint minio:9000 --minio-access-key minio --minio-secret-key minio123

# 3. IoT Raw -> Staging (ecrase la table)
docker compose run --rm --no-deps pipeline transformation/raw_to_staging.py \
  --bucket raw --dataset staging --table stg_sensor_timeseries \
  --source-name smart_building_dataset \
  --minio-endpoint minio:9000 --minio-access-key minio --minio-secret-key minio123 \
  --gcp-project-id smart-building-datalake --write-mode WRITE_TRUNCATE

# 4. API Raw -> Staging (ajoute l'air quality)
docker compose run --rm --no-deps pipeline transformation/api_to_staging.py \
  --bucket raw --dataset staging --table stg_sensor_timeseries \
  --source-name open_meteo_api --api-type air_quality \
  --minio-endpoint minio:9000 --minio-access-key minio --minio-secret-key minio123 \
  --gcp-project-id smart-building-datalake --write-mode WRITE_APPEND

# 5. Staging -> Curated
docker compose run --rm --no-deps pipeline transformation/staging_to_curated.py \
  --dataset-staging staging --dataset-curated curated \
  --staging-table stg_sensor_timeseries \
  --gcp-project-id smart-building-datalake --write-mode WRITE_TRUNCATE

# 6. Entrainement des modeles (au moins une fois)
docker compose run --rm --no-deps pipeline ml/train_isolation_forest.py \
  --dataset-curated curated --table cur_energy_by_device \
  --gcp-project-id smart-building-datalake \
  --bucket raw --model-prefix models --contamination 0.02 \
  --minio-endpoint minio:9000 --minio-access-key minio --minio-secret-key minio123

# 7. Scoring des anomalies -> cur_anomaly_scores
docker compose run --rm --no-deps pipeline ml/apply_models.py \
  --dataset-curated curated --features-table cur_energy_by_device \
  --scores-table cur_anomaly_scores --gcp-project-id smart-building-datalake \
  --bucket raw --model-prefix models \
  --minio-endpoint minio:9000 --minio-access-key minio --minio-secret-key minio123 \
  --write-mode WRITE_TRUNCATE
```

> **Ordre important.** L'étape 3 (`WRITE_TRUNCATE`) vide la table avant de la
> recharger et doit précéder l'étape 4 (`WRITE_APPEND`), afin que la qualité
> d'air soit ajoutée par-dessus l'IoT sans être écrasée.

---

## 8. Orchestration : les trois DAG

Le fichier `dags/smart_building_dag.py` définit trois DAG, dont la séparation
reflète la distinction entre préparation de la donnée, ingestion temps réel et
entraînement du modèle.

**`smart_building_full_pipeline`** (manuel) orchestre les six étapes du pipeline
de données : les deux ingestions (en parallèle, car indépendantes), les
transformations vers Staging et Curated, puis l'**inférence** du modèle
d'anomalies. Il applique des modèles existants, sans réentraîner.

**`smart_building_api_scheduled`** (`@hourly`) ré-ingère la qualité d'air des
dernières 24 h, avec des dates calculées dynamiquement à chaque exécution
(`{{ macros.ds_add(ds, -1) }}` et `{{ ds }}`). Il matérialise l'exigence de
scheduling du sujet.

**`smart_building_train_models`** (`@weekly`) **entraîne** les Isolation Forest.
Il est volontairement séparé du pipeline de données : l'entraînement est coûteux
et ne doit se faire que périodiquement, alors que l'inférence tourne à chaque
pipeline. C'est la séparation entraînement / inférence usuelle en production.

Airflow orchestre mais n'exécute pas la logique métier : chaque tâche lance le
script correspondant dans le conteneur `pipeline` via `docker compose run`.

---

## 9. API Gateway

Cinq endpoints, documentation interactive sur http://localhost:8000/docs.

| Endpoint | Description |
|----------|-------------|
| `GET /health` | État de l'API, de MinIO et de BigQuery |
| `GET /stats` | Compteurs globaux (objets Raw, lignes Staging, familles de métriques) |
| `GET /raw` | Inventaire des objets de la zone Raw, filtrable par préfixe |
| `GET /staging` | Lignes de la table Staging, paginées et filtrables |
| `GET /curated` | Lignes d'une table Curated (paramètre `table`), paginées |

```bash
curl "http://localhost:8000/stats"
curl "http://localhost:8000/raw?prefix=source_api/"
curl "http://localhost:8000/curated?table=cur_energy_by_device&entity=Fridge"
curl "http://localhost:8000/curated?table=cur_anomaly_scores&entity=Fridge"
```

Les requêtes BigQuery sont paramétrées et le nom de table Curated est validé
contre une liste blanche, afin de prévenir toute injection.

---

## 10. Zone Curated en détail

| Table | Grain | Contenu |
|-------|-------|---------|
| `cur_energy_by_device` | (event_time, device) | 6 mesures électriques pivotées, 7 appareils. Table de features du ML |
| `cur_environment_by_room` | (event_time, room) | température, humidité, présence, 3 pièces |
| `cur_daily_summary` | (day, scope, entity, metric, value) | agrégats journaliers, dont la qualité d'air |
| `cur_anomaly_scores` | (event_time, device, features, if_score, if_is_anomaly) | scores d'anomalies par appareil |

Trois cas particuliers sont gérés explicitement : l'agrégat bâtiment `GENERAL`
(valeurs nulles) est exclu du grain appareil ; les libellés de capteurs de
présence (`Lab Motion`…) sont normalisés vers la pièce canonique (`Lab`) ; la
qualité d'air, signal extérieur unique, est rattachée au niveau journalier.

---

## 11. Détection d'anomalies (Isolation Forest)

La détection est réalisée **par appareil** : chaque machine est jugée contre son
propre comportement normal, ce qui rend la détection pertinente (un pic de 200 W
est anormal pour une bouilloire au repos, routinier pour un frigo en dégivrage).

Le processus est découplé en deux scripts, suivant la séparation
entraînement / inférence :

- `train_isolation_forest.py` entraîne un Isolation Forest par appareil sur les
  6 features électriques de `cur_energy_by_device`, et sauvegarde chaque modèle
  dans MinIO sous `models/<Appareil>.pkl`.
- `apply_models.py` recharge ces modèles, score chaque ligne, et écrit la table
  `cur_anomaly_scores` (`if_score` continu, `if_is_anomaly` booléen).

Avec un taux de contamination de 0.02, le modèle marque environ 2 % des
observations comme anomalies (53 sur 2471 lignes, réparties sur les 7 appareils).
Les résultats sont interrogeables via `GET /curated?table=cur_anomaly_scores`.

---

## 12. Alignement avec le cours

Quelques choix d'architecture reprennent explicitement les notions du cours.

**Le ML comme enrichissement entre zones (Cours 1).** La détection d'anomalies
est un *scoring de qualité* appliqué en aval, sur des données déjà curées. Elle
correspond au type d'enrichissement Staging → Curated décrit dans le cours (aux
côtés de la classification, du clustering, des embeddings), et non à une
transformation de la donnée brute.

**Séparation entraînement / inférence (Cours 4).** L'entraînement, coûteux, vit
dans son propre DAG à cadence lente ; l'inférence, légère, est intégrée au
pipeline de données. Le pipeline exploite aussi le parallélisme des tâches
indépendantes (les deux ingestions), et la gestion d'erreurs via `retries` /
`retry_delay`.

**Gouvernance anti *data swamp* (Cours 1).** La persistance de la zone Raw,
l'auto-création idempotente du bucket, et les modes d'écriture maîtrisés
(`WRITE_TRUNCATE` / `WRITE_APPEND`) constituent la gouvernance minimale qui évite
que le lac ne devienne un marécage de données ingérables.

---

## 13. Limites et améliorations possibles

**Orchestration via `docker compose run`.** Airflow pilote le démon Docker de
l'hôte pour exécuter les scripts. C'est fonctionnel mais constitue un
anti-pattern en production ; une approche plus robuste utiliserait le
`DockerOperator`, un `KubernetesPodOperator` (un pod par tâche), ou des
`PythonOperator` important directement les fonctions.

**Idempotence.** L'ingestion API dépose un nouveau fichier à chaque run et la
transformation associée est en `WRITE_APPEND` ; sans réinitialisation du Raw, les
données d'air peuvent se dupliquer. En production, on garantirait l'idempotence
par un `MERGE` BigQuery sur une clé, ou par un partitionnement par date.

**Découpage des DAG.** En entreprise, on découperait plutôt par source et par
cadence de rafraîchissement, en reliant les DAG via les *Datasets* Airflow
(déclenchement piloté par la donnée). Les environnements dev / recette / prod se
gèrent par configuration, non par duplication.

**Gestion des secrets.** Les identifiants sont passés en clair aux scripts ; ils
devraient vivre dans les Connections et Variables d'Airflow, ou un gestionnaire
de secrets dédié.

**Observabilité.** Le pipeline ne remonte pas encore de métriques ni de contrôles
qualité automatisés (ex. alerter si le nombre de lignes chargées double).

---

## 14. Sources de données

### Dataset IoT (Zenodo)

| Propriété | Valeur |
|---|---|
| Nom | Dataset of IoT-Based Energy and Environmental Parameters in a Smart Building Infrastructure |
| Origine | Bâtiment M5, University of Sharjah |
| DOI | [10.5281/zenodo.12750891](https://doi.org/10.5281/zenodo.12750891) |
| Licence | CC-BY 4.0 |
| Format | 53 fichiers CSV (mesures par appareil/pièce) |
| Période | 1er janvier -> 20 juin 2024 |

Familles de mesures : puissance (W), puissance apparente (VA), tension (V),
courant (A), énergie journalière et cumulée (kWh) par appareil ; température,
humidité, présence par pièce.

### Open-Meteo Air Quality API

| Propriété | Valeur |
|---|---|
| URL de base | `https://air-quality-api.open-meteo.com/v1/air-quality` |
| Licence | Gratuite, sans clé |
| Format | JSON horaire |
| Coordonnées | `latitude=25.3488`, `longitude=55.4054` (Sharjah) |
| Variables | pm2_5, pm10, carbon_monoxide, nitrogen_dioxide, ozone, european_aqi |

---

*Projet réalisé dans le cadre du cours Data Lakes & Data Integration — EFREI 2025-2026.*