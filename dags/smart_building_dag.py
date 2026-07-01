"""
DAG Airflow d'orchestration du data lake Smart Building.

Trois DAG cohabitent, refletant la separation entre preparation de la donnee,
scheduling de la source temps reel, et entrainement du modele ML :

  - smart_building_full_pipeline (manuel) : ingestion des deux sources,
    transformations Raw -> Staging -> Curated, puis APPLICATION du modele
    d'anomalies (inference legere avec les modeles deja entraines).
  - smart_building_api_scheduled (@hourly) : re-ingestion planifiee de la
    qualite d'air Open-Meteo, dates calculees dynamiquement.
  - smart_building_train_models (@weekly) : ENTRAINEMENT des Isolation Forest.
    Separe du pipeline de donnees car l'entrainement est couteux et ne doit se
    faire que periodiquement, alors que l'inference tourne a chaque pipeline.

Pattern repris du TP4 : extract >> transform >> load, avec dependances explicites.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Arguments par defaut communs a toutes les taches.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Identifiants MinIO et GCP passes aux scripts. En production on les sortirait
# en Variables Airflow ; ici on reste aligne sur les valeurs du projet.
MINIO_ARGS = (
    "--minio-endpoint minio:9000 "
    "--minio-access-key minio "
    "--minio-secret-key minio123"
)
GCP_PROJECT = "smart-building-datalake"

# Prefixe commun : on execute chaque script dans le conteneur pipeline.
# --rm nettoie le conteneur ephemere apres chaque tache.
RUN = (
    "docker compose -p smart-building-datalake --project-directory /opt/project "
    "-f /opt/project/docker-compose.yml run --rm --no-deps "
    "-e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-service-account.json "
    "pipeline"
)


# DAG 1 : pipeline complet de donnees + inference (manuel)

with DAG(
    dag_id="smart_building_full_pipeline",
    default_args=default_args,
    description="Pipeline complet Raw -> Staging -> Curated + scoring anomalies.",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # declenchement manuel
    catchup=False,
    tags=["smart-building", "full"],
) as full_dag:

    ingest_dataset = BashOperator(
        task_id="ingest_dataset_to_raw",
        bash_command=(
            f"{RUN} ingestion/ingest_dataset.py "
            f"--dataset-root data --bucket raw "
            f"--source-name smart_building_dataset {MINIO_ARGS}"
        ),
    )

    ingest_api = BashOperator(
        task_id="ingest_api_to_raw",
        bash_command=(
            f"{RUN} ingestion/ingest_api.py "
            f"--bucket raw --source-name open_meteo_api --api-type air_quality "
            f"--latitude 25.3488 --longitude 55.4054 "
            f"--start-date 2024-01-01 --end-date 2024-06-20 --timezone GMT {MINIO_ARGS}"
        ),
    )

    raw_to_staging = BashOperator(
        task_id="raw_to_staging",
        bash_command=(
            f"{RUN} transformation/raw_to_staging.py "
            f"--bucket raw --dataset staging --table stg_sensor_timeseries "
            f"--source-name smart_building_dataset {MINIO_ARGS} "
            f"--gcp-project-id {GCP_PROJECT} --write-mode WRITE_TRUNCATE"
        ),
    )

    api_to_staging = BashOperator(
        task_id="api_to_staging",
        bash_command=(
            f"{RUN} transformation/api_to_staging.py "
            f"--bucket raw --dataset staging --table stg_sensor_timeseries "
            f"--source-name open_meteo_api --api-type air_quality {MINIO_ARGS} "
            f"--gcp-project-id {GCP_PROJECT} --write-mode WRITE_APPEND"
        ),
    )

    staging_to_curated = BashOperator(
        task_id="staging_to_curated",
        bash_command=(
            f"{RUN} transformation/staging_to_curated.py "
            f"--dataset-staging staging --dataset-curated curated "
            f"--staging-table stg_sensor_timeseries "
            f"--gcp-project-id {GCP_PROJECT} --write-mode WRITE_TRUNCATE"
        ),
    )

    # Inference seule : applique les modeles deja entraines (pas d'entrainement ici).
    apply_anomaly_models = BashOperator(
        task_id="apply_anomaly_models",
        bash_command=(
            f"{RUN} ml/apply_models.py "
            f"--dataset-curated curated --features-table cur_energy_by_device "
            f"--scores-table cur_anomaly_scores --gcp-project-id {GCP_PROJECT} "
            f"--bucket raw --model-prefix models {MINIO_ARGS} "
            f"--write-mode WRITE_TRUNCATE"
        ),
    )

    # Dependances : les deux sources alimentent Raw, puis Staging dans le bon
    # ordre (truncate fichier avant append API), puis Curated, puis scoring.
    ingest_dataset >> raw_to_staging
    ingest_api >> api_to_staging
    raw_to_staging >> api_to_staging >> staging_to_curated >> apply_anomaly_models


# DAG 2 : ingestion API planifiee (@hourly)

with DAG(
    dag_id="smart_building_api_scheduled",
    default_args=default_args,
    description="Ingestion planifiee de la qualite d'air Open-Meteo (dernieres 24h).",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",  # scheduling demande par le sujet
    catchup=False,
    tags=["smart-building", "api", "scheduled"],
) as api_dag:

    ingest_api_recent = BashOperator(
        task_id="ingest_api_recent",
        bash_command=(
            f"{RUN} ingestion/ingest_api.py "
            f"--bucket raw --source-name open_meteo_api --api-type air_quality "
            f"--latitude 25.3488 --longitude 55.4054 "
            f"--start-date {{{{ macros.ds_add(ds, -1) }}}} --end-date {{{{ ds }}}} "
            f"--timezone GMT {MINIO_ARGS}"
        ),
    )

    api_recent_to_staging = BashOperator(
        task_id="api_recent_to_staging",
        bash_command=(
            f"{RUN} transformation/api_to_staging.py "
            f"--bucket raw --dataset staging --table stg_sensor_timeseries "
            f"--source-name open_meteo_api --api-type air_quality {MINIO_ARGS} "
            f"--gcp-project-id {GCP_PROJECT} --write-mode WRITE_APPEND"
        ),
    )

    ingest_api_recent >> api_recent_to_staging


# DAG 3 : entrainement des modeles (@weekly, cadence lente)

with DAG(
    dag_id="smart_building_train_models",
    default_args=default_args,
    description="Entrainement periodique des Isolation Forest par appareil.",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",  # entrainement rare, decouple du pipeline de donnees
    catchup=False,
    tags=["smart-building", "ml", "training"],
) as train_dag:

    train_anomaly_models = BashOperator(
        task_id="train_anomaly_models",
        bash_command=(
            f"{RUN} ml/train_isolation_forest.py "
            f"--dataset-curated curated --table cur_energy_by_device "
            f"--gcp-project-id {GCP_PROJECT} "
            f"--bucket raw --model-prefix models --contamination 0.02 {MINIO_ARGS}"
        ),
    )