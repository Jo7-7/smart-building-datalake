"""
DAG Airflow d'orchestration du data lake Smart Building.

Le DAG enchaine les etapes du pipeline medaillon Raw -> Staging -> Curated en
appelant les scripts deja valides du projet, executes dans le conteneur
`pipeline` via `docker compose run`. Airflow orchestre, l'image pipeline execute.

Deux logiques de planification cohabitent :
  - le pipeline complet (fichier + API) peut etre declenche manuellement ;
  - l'ingestion de l'API qualite d'air est planifiee a intervalle regulier,
    conformement a l'attendu du sujet sur le scheduling.

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

# ───────────────── DAG 1 : pipeline complet (manuel) ─────────────────

with DAG(
    dag_id="smart_building_full_pipeline",
    default_args=default_args,
    description="Pipeline complet Raw -> Staging -> Curated (fichier + API).",
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

    # Dependances : les deux sources alimentent Raw, puis Staging dans le bon
    # ordre (truncate fichier avant append API), puis Curated.
    ingest_dataset >> raw_to_staging
    ingest_api >> api_to_staging
    raw_to_staging >> api_to_staging >> staging_to_curated


# ───────────────── DAG 2 : ingestion API planifiee ─────────────────

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