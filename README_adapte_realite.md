# 🏢 Smart Building — Pipeline d'Ingestion & Data Warehouse

Pipeline de données end-to-end pour collecter, stocker et transformer des données de bâtiment intelligent depuis un dataset fichier et des APIs externes vers **BigQuery**, avec une architecture **Médaillon** `Raw → Staging → Curated` et **MinIO** comme zone Raw objet. [file:436][file:38]

Le projet répond au cahier des charges EFREI en couvrant l’ingestion, la transformation entre zones, la reproductibilité du pipeline et l’exposition future via une API Gateway. [file:38]

---

## 📋 Présentation

Le projet **Smart Building** vise à centraliser et fiabiliser des données opérationnelles de bâtiment intelligent, notamment des mesures énergétiques, environnementales et de contexte, dans une architecture de Data Lake simple à expliquer et à exécuter. [file:436]

Dans votre réalité actuelle, l’architecture cible repose sur :

- un **dataset fichier** local comme première source ; [file:38][file:437]
- **deux APIs externes** comme sources complémentaires ; [file:436][file:437]
- **MinIO** pour la zone **Raw** ; [file:436]
- **BigQuery** pour les zones **Staging** et **Curated**. [file:436][file:437]

---

## 🎯 Objectifs

- Collecter les données depuis des fichiers locaux CSV/JSON et depuis 2 APIs externes. [file:436]
- Déposer les données brutes dans **MinIO** afin de conserver une couche Raw immuable et rejouable. [file:436]
- Charger et typer les données dans **BigQuery** pour la zone **Staging**. [file:436][file:437]
- Construire une zone **Curated** analytique à partir de transformations SQL ou Python. [file:436][file:437]
- Fournir une base exploitable pour l’analyse, le reporting et, à terme, le Machine Learning et l’API Gateway demandée dans le projet. [file:38][file:437]

---

## 🧱 Architecture retenue

Votre dépôt et votre schéma d’architecture montrent une orientation claire : **Raw sur stockage objet**, puis **Staging et Curated dans BigQuery**. [file:436][file:437]

```text
Sources fichiers + APIs
        |
        v
Scripts d'ingestion Python
        |
        v
MinIO (Raw / Bronze)
        |
        v
BigQuery dataset staging (Silver)
        |
        v
BigQuery dataset curated (Gold)
```

Cette architecture est cohérente avec les attentes du projet, qui imposent une zone Raw sur S3 ou équivalent, tout en laissant le reste libre tant qu’il est justifié. [file:38]

---

## 🔄 Flux de données

Le flux réel à documenter dans votre README est le suivant :

1. Les scripts Python lisent les fichiers sources locaux et appellent les APIs externes. [file:436][file:437]
2. Les payloads bruts sont stockés dans **MinIO** sans transformation majeure. [file:436]
3. Les objets Raw sont relus pour être chargés dans **BigQuery Staging**, avec typage, normalisation et déduplication. [file:436][file:437]
4. Les données **Staging** sont transformées vers **Curated** pour créer des tables prêtes à l’analyse. [file:436][file:437]

---

## 🗂️ Sources de données

À ce stade, votre documentation actuelle décrit les sources de manière générique. Elle parle d’un ensemble de **fichiers locaux (CSV/JSON)** et de **2 APIs externes**, sans encore figer publiquement le détail métier dans le README fourni. [file:436][file:437]

Pour rester fidèle à votre réalité actuelle, il vaut mieux documenter les sources comme suit :

| Source | Type | État de documentation actuel |
| --- | --- | --- |
| Source 1 | Dataset fichier local (CSV/JSON) | Confirmée dans le README et l’architecture [file:436][file:437] |
| Source 2 | API externe n°1 | Confirmée dans le README et l’architecture [file:436][file:437] |
| Source 3 | API externe n°2 | Confirmée dans le README et l’architecture [file:436][file:437] |

Cette formulation évite d’inventer des détails non stabilisés, tout en restant conforme aux exigences de deux familles de sources demandées par le projet. [file:38]

---

## 🥉 Zone Raw — MinIO

La zone **Raw** est stockée dans **MinIO**, utilisé comme stockage objet compatible S3. [file:436]

Son rôle est de conserver les données brutes telles qu’elles sont reçues afin de garantir :

- la traçabilité ;
- l’auditabilité ;
- la rejouabilité des transformations. [file:436]

Exemples de contenu Raw documentés dans votre README :

- fichiers sources déposés via `ingest_files.py` ; [file:436]
- réponses d’APIs déposées via `ingest_api.py`. [file:436]

---

## 🥈 Zone Staging — BigQuery

La zone **Staging** est hébergée dans le dataset BigQuery `staging`. [file:436][file:437]

Dans votre documentation actuelle, cette couche est décrite comme une zone intermédiaire contenant des données :

- typées ;
- dédupliquées ;
- normalisées ;
- encore proches des sources. [file:436][file:437]

Autrement dit, **Staging** n’est pas encore la couche métier finale ; c’est la couche de structuration et de fiabilisation. [file:436]

---

## 🥇 Zone Curated — BigQuery

La zone **Curated** est hébergée dans le dataset BigQuery `curated`. [file:436][file:437]

Elle contient les tables finales destinées à :

- l’analyse ;
- le reporting ;
- une éventuelle consommation BI ;
- une exploitation ML ou notebook par la suite. [file:436][file:437]

C’est donc la couche qui matérialise la vision analytique stable du projet. [file:436]

---

## 🛠️ Stack technique réelle

La stack qui ressort clairement des fichiers fournis est la suivante :

| Composant | Technologie | Usage |
| --- | --- | --- |
| Langage | Python 3.11+ | Ingestion et transformation [file:436] |
| Raw | MinIO | Stockage objet brut [file:436] |
| Data Warehouse | Google BigQuery | Staging et Curated [file:436][file:437] |
| Conteneurisation | Docker / Docker Compose | Exécution reproductible [file:436] |
| Accès objet | `boto3` ou `minio` | Lecture/écriture MinIO [file:436] |
| Accès BigQuery | `google-cloud-bigquery` | Chargement et requêtes [file:436] |
| Traitement tabulaire | `pandas`, `pyarrow` | Préparation et typage [file:436] |
| Requêtes HTTP | `requests` ou `httpx` | Appels API [file:436] |

---

## 📁 Structure du dépôt

La structure documentée actuellement dans votre README est la suivante : [file:436]

```text
smart-building/
├── README.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
├── config/
│   └── settings.py
├── src/
│   ├── ingestion/
│   │   ├── ingest_files.py
│   │   └── ingest_api.py
│   ├── transformation/
│   │   ├── load_staging.py
│   │   └── build_curated.py
│   └── utils/
│       ├── minio_client.py
│       └── bq_client.py
├── sql/
│   ├── staging/
│   └── curated/
├── data/
│   └── sources/
└── credentials/
    └── gcp-service-account.json
```

Cette structure est cohérente avec l’architecture visée et suffisamment claire pour répondre à l’exigence de documentation du projet. [file:38][file:436]

---

## ⚙️ Exécution du pipeline

Le README actuel décrit déjà une exécution séquencée alignée avec l’architecture médaillon. [file:436]

Ordre logique :

1. `src/ingestion/ingest_files.py` pour les fichiers locaux vers Raw ; [file:436]
2. `src/ingestion/ingest_api.py` pour les APIs vers Raw ; [file:436]
3. `src/transformation/load_staging.py` pour Raw vers Staging ; [file:436]
4. `src/transformation/build_curated.py` pour Staging vers Curated. [file:436]

C’est exactement cette séquence qu’il faut mettre en avant comme “réalité projet” plutôt qu’une version plus ambitieuse non encore implémentée. [file:436]

---

## 🌐 API et orchestration

Le sujet EFREI demande une **pipeline d’intégration continue**, avec par exemple **Apache Airflow**, ainsi qu’une **API Gateway** avec les endpoints `/raw`, `/staging`, `/curated`, `/health` et `/stats`. [file:38]

Dans les fichiers fournis, l’orchestration apparaît dans le schéma comme une brique prévue, mais votre README actuel est surtout centré sur l’ingestion, la transformation, Docker, MinIO et BigQuery. [file:436][file:437]

La formulation la plus juste est donc :

- **orchestration prévue / en cours d’intégration** ;
- **API Gateway prévue pour exposer les zones du lac**. [file:38][file:437]

---

## ✅ Positionnement réaliste du projet

Pour “adapter à votre réalité”, il faut éviter de surpromettre. Les fichiers montrent aujourd’hui un projet centré sur un **pipeline batch reproductible** avec **MinIO + BigQuery + Docker**, et non encore un produit complet avec toutes les briques finales opérationnelles. [file:436][file:437]

La présentation la plus honnête est donc :

- architecture médaillon en place ; [file:436]
- flux Raw → Staging → Curated défini ; [file:436][file:437]
- scripts d’ingestion et de transformation structurés ; [file:436]
- BigQuery comme entrepôt analytique ; [file:436]
- API Gateway et orchestration avancée comme prochaines étapes. [file:38][file:437]

---

## 🚀 Suite logique

Les prochaines briques à documenter ou finaliser de manière cohérente avec votre dépôt sont :

- la définition précise des schémas de tables **Staging** et **Curated** ;
- la convention de nommage des objets **Raw** dans MinIO ;
- la création des scripts SQL de transformation ;
- l’ajout d’une orchestration Airflow ;
- l’implémentation de l’API Gateway demandée par le sujet. [file:38][file:436][file:437]
