from datetime import datetime
from typing import Optional, List, Any

import os

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel

from google.cloud import bigquery
from minio import Minio
from minio.error import S3Error


# ───────────────── Config & Clients ─────────────────

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "smart-building-datalake")
BQ_STAGING_DATASET = os.getenv("BQ_STAGING_DATASET", "staging")
BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_sensor_timeseries")
BQ_CURATED_DATASET = os.getenv("BQ_CURATED_DATASET", "curated")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw")

# Tables Curated autorisées, pour éviter toute injection via le paramètre `table`.
CURATED_TABLES = {
    "cur_energy_by_device",
    "cur_environment_by_room",
    "cur_daily_summary",
    "cur_anomaly_scores",
}
app = FastAPI(
    title="Smart Building Data Lake API",
    description="API Gateway pour les couches Raw / Staging / Curated du data lake Smart Building.",
    version="1.0.0",
)


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT_ID)


def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


# ───────────────── Modèles de réponse ─────────────────

class HealthStatus(BaseModel):
    status: str
    timestamp_utc: datetime
    checks: dict


class StatsResponse(BaseModel):
    raw_objects_count: int
    staging_rows_count: int
    metric_families: List[str]


class StagingRow(BaseModel):
    event_time: datetime
    metric_family: str
    metric_value: Optional[float]
    metric_unit: str
    entity_name: Optional[str]
    source_file_name: Optional[str]
    source_folder: Optional[str]
    ingested_at: Optional[datetime]


class StagingPage(BaseModel):
    rows: List[StagingRow]
    page: int
    page_size: int
    total_rows: int


class RawObject(BaseModel):
    object_name: str
    size_bytes: int
    last_modified: Optional[datetime]


class RawPage(BaseModel):
    objects: List[RawObject]
    page: int
    page_size: int
    total_objects: int


class CuratedPage(BaseModel):
    table: str
    rows: List[dict]
    page: int
    page_size: int
    total_rows: int


# ───────────────── Endpoints ─────────────────


@app.get("/health", response_model=HealthStatus, tags=["meta"])
def health_check():
    """
    Vérifie l'état global de l'API, de MinIO et de BigQuery.
    """
    checks = {}

    # Check MinIO
    try:
        minio_client = get_minio_client()
        # simple opération : lister au moins un objet ou vérifier l'existence du bucket
        checks["minio"] = {
            "status": "ok" if minio_client.bucket_exists(MINIO_RAW_BUCKET) else "missing_bucket",
            "bucket": MINIO_RAW_BUCKET,
        }
    except S3Error as e:
        checks["minio"] = {"status": "error", "detail": str(e)}
    except Exception as e:
        checks["minio"] = {"status": "error", "detail": str(e)}

    # Check BigQuery
    try:
        client = get_bq_client()
        table_id = f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}"
        client.get_table(table_id)  # simple GET
        checks["bigquery"] = {"status": "ok", "staging_table": table_id}
    except Exception as e:
        checks["bigquery"] = {"status": "error", "detail": str(e)}

    overall_status = "ok" if all(c.get("status") == "ok" for c in checks.values()) else "degraded"

    return HealthStatus(
        status=overall_status,
        timestamp_utc=datetime.utcnow(),
        checks=checks,
    )


@app.get("/stats", response_model=StatsResponse, tags=["meta"])
def stats():
    raw_objects_count = 0

    minio_client = get_minio_client()
    try:
        if minio_client.bucket_exists(MINIO_RAW_BUCKET):
            for _ in minio_client.list_objects(bucket_name=MINIO_RAW_BUCKET, recursive=True):
                raw_objects_count += 1
    except Exception:
        raw_objects_count = 0

    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}"

    query = f"""
    SELECT
      COUNT(*) AS total_rows,
      ARRAY_AGG(DISTINCT metric_family IGNORE NULLS) AS metric_families
    FROM `{table_id}`
    """
    result = list(client.query(query))[0]

    return StatsResponse(
        raw_objects_count=raw_objects_count,
        staging_rows_count=int(result["total_rows"]),
        metric_families=result["metric_families"] or [],
    )


@app.get("/raw", response_model=RawPage, tags=["raw"])
def get_raw(
    prefix: Optional[str] = Query(
        default=None,
        description="Filtre par préfixe d'objet, ex. 'source_dataset/' ou 'source_api/'.",
    ),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
):
    """
    Liste les objets de la zone Raw (bucket MinIO) avec pagination et filtre par préfixe.

    On expose l'inventaire des objets (nom, taille, date) plutôt que leur contenu
    binaire, ce qui correspond au rôle d'exploration attendu pour cet endpoint.
    """
    minio_client = get_minio_client()

    if not minio_client.bucket_exists(MINIO_RAW_BUCKET):
        raise HTTPException(
            status_code=503,
            detail=f"Bucket Raw '{MINIO_RAW_BUCKET}' introuvable. Lancez l'ingestion d'abord.",
        )

    # On matérialise la liste pour pouvoir compter et paginer côté API.
    try:
        all_objects = list(
            minio_client.list_objects(
                bucket_name=MINIO_RAW_BUCKET,
                prefix=prefix or None,
                recursive=True,
            )
        )
    except S3Error as e:
        raise HTTPException(status_code=502, detail=f"Erreur MinIO : {e}")

    total_objects = len(all_objects)
    offset = (page - 1) * page_size
    window = all_objects[offset: offset + page_size]

    objects = [
        RawObject(
            object_name=obj.object_name,
            size_bytes=obj.size or 0,
            last_modified=obj.last_modified,
        )
        for obj in window
    ]

    return RawPage(
        objects=objects,
        page=page,
        page_size=page_size,
        total_objects=total_objects,
    )


@app.get("/staging", response_model=StagingPage, tags=["staging"])
def get_staging(
    metric_family: Optional[str] = Query(default=None),
    entity_name: Optional[str] = Query(default=None),
    start_time: Optional[datetime] = Query(default=None),
    end_time: Optional[datetime] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
):
    """
    Retourne des lignes de la table staging.stg_sensor_timeseries avec pagination et filtres basiques.
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}"

    where_clauses = []
    params = []

    if metric_family:
        where_clauses.append("metric_family = @metric_family")
        params.append(bigquery.ScalarQueryParameter("metric_family", "STRING", metric_family))

    if entity_name:
        where_clauses.append("entity_name = @entity_name")
        params.append(bigquery.ScalarQueryParameter("entity_name", "STRING", entity_name))

    if start_time:
        where_clauses.append("event_time >= @start_time")
        params.append(bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time))

    if end_time:
        where_clauses.append("event_time <= @end_time")
        params.append(bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time))

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    offset = (page - 1) * page_size

    # Total rows pour la pagination
    count_query = bigquery.QueryJobConfig(query_parameters=params)
    count_sql = f"""
    SELECT COUNT(*) AS total_rows
    FROM `{table_id}`
    {where_sql}
    """

    count_result = list(client.query(count_sql, job_config=count_query))
    total_rows = int(count_result[0]["total_rows"]) if count_result else 0

    if total_rows == 0:
        return StagingPage(rows=[], page=page, page_size=page_size, total_rows=0)

    # Page de données
    data_query = bigquery.QueryJobConfig(query_parameters=params)
    data_sql = f"""
    SELECT
      event_time,
      metric_family,
      metric_value,
      metric_unit,
      entity_name,
      source_file_name,
      source_folder,
      ingested_at
    FROM `{table_id}`
    {where_sql}
    ORDER BY event_time
    LIMIT @limit OFFSET @offset
    """

    # Ajouter limit/offset aux paramètres
    data_query.query_parameters = params + [
        bigquery.ScalarQueryParameter("limit", "INT64", page_size),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
    ]

    rows = []
    for row in client.query(data_sql, job_config=data_query):
        rows.append(
            StagingRow(
                event_time=row["event_time"],
                metric_family=row["metric_family"],
                metric_value=row["metric_value"],
                metric_unit=row["metric_unit"],
                entity_name=row.get("entity_name"),
                source_file_name=row.get("source_file_name"),
                source_folder=row.get("source_folder"),
                ingested_at=row.get("ingested_at"),
            )
        )

    return StagingPage(
        rows=rows,
        page=page,
        page_size=page_size,
        total_rows=total_rows,
    )


@app.get("/curated", response_model=CuratedPage, tags=["curated"])
def get_curated(
    table: str = Query(
        default="cur_energy_by_device",
        description="Table Curated à lire : cur_energy_by_device, cur_environment_by_room ou cur_daily_summary.",
    ),
    entity: Optional[str] = Query(
        default=None,
        description="Filtre sur l'entité (device, room ou entity selon la table).",
    ),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
):
    """
    Retourne des lignes d'une table Curated, avec pagination et filtre optionnel par entité.

    Le paramètre `table` est validé contre une liste blanche pour éviter toute injection,
    puisqu'un nom de table ne peut pas être passé en paramètre lié dans BigQuery.
    """
    if table not in CURATED_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"Table Curated inconnue : {table}. Valeurs possibles : {sorted(CURATED_TABLES)}.",
        )

    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_CURATED_DATASET}.{table}"

    # La colonne d'entité dépend de la table ciblée.
    entity_column = {
        "cur_energy_by_device": "device",
        "cur_environment_by_room": "room",
        "cur_daily_summary": "entity",
        "cur_anomaly_scores": "device",
    }[table]

    where_sql = ""
    params = []
    if entity:
        where_sql = f"WHERE {entity_column} = @entity"
        params.append(bigquery.ScalarQueryParameter("entity", "STRING", entity))

    # Total rows pour la pagination
    count_sql = f"SELECT COUNT(*) AS total_rows FROM `{table_id}` {where_sql}"
    count_cfg = bigquery.QueryJobConfig(query_parameters=params)
    count_result = list(client.query(count_sql, job_config=count_cfg))
    total_rows = int(count_result[0]["total_rows"]) if count_result else 0

    if total_rows == 0:
        return CuratedPage(table=table, rows=[], page=page, page_size=page_size, total_rows=0)

    offset = (page - 1) * page_size

    data_sql = f"""
    SELECT *
    FROM `{table_id}`
    {where_sql}
    LIMIT @limit OFFSET @offset
    """
    data_cfg = bigquery.QueryJobConfig(
        query_parameters=params + [
            bigquery.ScalarQueryParameter("limit", "INT64", page_size),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
    )

    rows = [dict(row.items()) for row in client.query(data_sql, job_config=data_cfg)]

    return CuratedPage(
        table=table,
        rows=rows,
        page=page,
        page_size=page_size,
        total_rows=total_rows,
    )