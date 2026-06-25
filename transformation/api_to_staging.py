"""
Chargement des payloads JSON Raw de l'API Open-Meteo depuis MinIO vers la
table Staging BigQuery, dans le MEME format long unifie que les capteurs IoT.

Ce script lit les objets JSON de la zone Raw deposes par ingestion/ingest_api.py
(prefixe source_api/), deplie les tableaux horaires en lignes longues, normalise
l'horodatage en UTC, puis append le tout dans stg_sensor_timeseries.

Choix d'architecture : on ne cree PAS de table large separee. La qualite d'air
arrive dans la meme table longue avec des metric_family prefixees (air_pm2_5,
air_european_aqi, ...) et entity_name = OUTDOOR. Resultat, les endpoints /stats
et /staging exposent la nouvelle source sans aucune modification cote API.

Usage:
    python transformation/api_to_staging.py \
        --bucket raw \
        --dataset staging \
        --table stg_sensor_timeseries \
        --source-name open_meteo_api \
        --api-type air_quality \
        --minio-endpoint minio:9000 \
        --minio-access-key minio \
        --minio-secret-key minio123 \
        --gcp-project-id smart-building-datalake \
        --write-mode WRITE_APPEND
"""

import argparse
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from minio import Minio


# Prefixe ajoute devant chaque variable pour identifier clairement la source API
# et eviter toute collision avec les familles IoT existantes.
API_FAMILY_PREFIX = {
    "air_quality": "air_",
    "weather": "weather_",
}

# Unites de secours si le payload ne fournit pas hourly_units pour une variable.
FALLBACK_UNITS = {
    "pm2_5": "ug/m3",
    "pm10": "ug/m3",
    "carbon_monoxide": "ug/m3",
    "nitrogen_dioxide": "ug/m3",
    "sulphur_dioxide": "ug/m3",
    "ozone": "ug/m3",
    "european_aqi": "EAQI",
    "temperature_2m": "C",
    "relative_humidity_2m": "%",
    "surface_pressure": "hPa",
    "precipitation": "mm",
    "wind_speed_10m": "km/h",
    "cloud_cover": "%",
    "shortwave_radiation": "W/m2",
}


def parse_args():
    """
    Parse les arguments de ligne de commande.
    """
    parser = argparse.ArgumentParser(
        description="Charge les JSON API Raw MinIO vers la table Staging BigQuery (format long)."
    )
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--table", type=str, default="stg_sensor_timeseries")
    parser.add_argument("--source-name", type=str, required=True)
    parser.add_argument(
        "--api-type",
        type=str,
        choices=["air_quality", "weather"],
        required=True,
    )
    parser.add_argument("--entity-name", type=str, default="OUTDOOR")
    parser.add_argument("--minio-endpoint", type=str, required=True)
    parser.add_argument("--minio-access-key", type=str, required=True)
    parser.add_argument("--minio-secret-key", type=str, required=True)
    parser.add_argument("--secure", action="store_true")
    parser.add_argument("--gcp-project-id", type=str, required=True)
    parser.add_argument(
        "--write-mode",
        type=str,
        choices=["WRITE_APPEND", "WRITE_TRUNCATE"],
        default="WRITE_APPEND",
    )
    return parser.parse_args()


def build_raw_prefix(source_name, api_type):
    """
    Construit le prefixe Raw a explorer, coherent avec ingest_api.build_raw_object_key.
    """
    return f"source_api/source_name={source_name}/api_type={api_type}/"


def list_raw_json_objects(client, bucket, prefix):
    """
    Liste les payloads JSON Raw correspondant a la source API.
    """
    object_names = []
    for obj in client.list_objects(bucket_name=bucket, prefix=prefix, recursive=True):
        if obj.object_name.endswith(".json"):
            object_names.append(obj.object_name)
    return sorted(object_names)


def read_json_from_minio(client, bucket, object_name):
    """
    Lit un payload JSON stocke dans MinIO et retourne un dict Python.
    """
    response = client.get_object(bucket_name=bucket, object_name=object_name)
    try:
        payload = response.read()
    finally:
        response.close()
        response.release_conn()
    return json.loads(payload.decode("utf-8"))


def transform_api_payload(payload, api_type, entity_name, source_file_name):
    """
    Transforme un payload Open-Meteo vers le schema Staging long unifie.

    Deplie le bloc hourly (un tableau time + un tableau par variable) en lignes
    (event_time, metric_family, metric_value, ...), et ramene event_time en UTC
    a partir de utc_offset_seconds pour s'aligner sur les capteurs IoT.
    """
    if "hourly" not in payload or "time" not in payload["hourly"]:
        raise ValueError("Payload sans bloc hourly.time exploitable")

    hourly = payload["hourly"]
    units = payload.get("hourly_units", {})
    offset_seconds = int(payload.get("utc_offset_seconds", 0))
    prefix = API_FAMILY_PREFIX[api_type]

    # Horodatage : Open-Meteo renvoie une heure locale naive selon la timezone
    # demandee. On retranche l'offset pour obtenir l'UTC, puis on tague UTC.
    event_time = pd.to_datetime(pd.Series(hourly["time"]), errors="coerce")
    event_time = event_time - pd.to_timedelta(offset_seconds, unit="s")
    event_time = event_time.dt.tz_localize("UTC")

    frames = []
    variables = [key for key in hourly.keys() if key != "time"]

    if not variables:
        raise ValueError("Aucune variable de mesure dans le bloc hourly")

    ingested_at = pd.Timestamp.now("UTC")

    for var in variables:
        values = pd.to_numeric(pd.Series(hourly[var]), errors="coerce")

        # Securite : ne traite que les variables dont la longueur colle au temps.
        if len(values) != len(event_time):
            print(f"[WARN] Variable ignoree (taille incoherente) : {var}")
            continue

        metric_unit = units.get(var) or FALLBACK_UNITS.get(var, "")

        frame = pd.DataFrame(
            {
                "event_time": event_time.values,
                "metric_family": f"{prefix}{var}",
                "metric_value": values.values,
                "metric_unit": metric_unit,
                "entity_name": entity_name,
                "source_file_name": source_file_name,
                "source_folder": "source_api",
                "ingested_at": ingested_at,
            }
        )
        frames.append(frame)

    if not frames:
        raise ValueError("Aucune variable exploitable apres controle de coherence")

    out = pd.concat(frames, ignore_index=True)
    return out[
        [
            "event_time",
            "metric_family",
            "metric_value",
            "metric_unit",
            "entity_name",
            "source_file_name",
            "source_folder",
            "ingested_at",
        ]
    ]


def build_staging_dataframe(client, bucket, object_names, api_type, entity_name):
    """
    Construit le DataFrame Staging global a partir des payloads JSON Raw.
    """
    frames = []

    for object_name in object_names:
        source_file_name = Path(object_name).name
        try:
            payload = read_json_from_minio(client, bucket, object_name)
            stg_df = transform_api_payload(
                payload=payload,
                api_type=api_type,
                entity_name=entity_name,
                source_file_name=source_file_name,
            )
            frames.append(stg_df)
            print(f"[OK] Transforme : {object_name} ({len(stg_df)} lignes)")
        except Exception as e:
            print(f"[WARN] Objet ignore : {object_name} | raison : {e}")
            continue

    columns = [
        "event_time",
        "metric_family",
        "metric_value",
        "metric_unit",
        "entity_name",
        "source_file_name",
        "source_folder",
        "ingested_at",
    ]

    if not frames:
        return pd.DataFrame(columns=columns)

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates()
    return df


def load_to_bigquery(df, project_id, dataset, table, write_mode):
    """
    Charge un DataFrame pandas dans BigQuery avec le meme schema que le Staging IoT.
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        schema=[
            bigquery.SchemaField("event_time", "TIMESTAMP"),
            bigquery.SchemaField("metric_family", "STRING"),
            bigquery.SchemaField("metric_value", "FLOAT64"),
            bigquery.SchemaField("metric_unit", "STRING"),
            bigquery.SchemaField("entity_name", "STRING"),
            bigquery.SchemaField("source_file_name", "STRING"),
            bigquery.SchemaField("source_folder", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def main():
    """
    Point d'entree principal du script.
    """
    args = parse_args()
    started_at = datetime.now(timezone.utc)

    minio_client = Minio(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=args.secure,
    )

    prefix = build_raw_prefix(args.source_name, args.api_type)

    print("=== Raw API vers Staging ===")
    print(f"Bucket source : {args.bucket}")
    print(f"Prefixe source: {prefix}")
    print(f"Dataset BQ    : {args.dataset}")
    print(f"Table BQ      : {args.table}")
    print(f"API type      : {args.api_type}")
    print(f"Entity        : {args.entity_name}")
    print(f"Write mode    : {args.write_mode}")

    object_names = list_raw_json_objects(
        client=minio_client,
        bucket=args.bucket,
        prefix=prefix,
    )

    print(f"\nObjets JSON trouves : {len(object_names)}")

    stg_df = build_staging_dataframe(
        client=minio_client,
        bucket=args.bucket,
        object_names=object_names,
        api_type=args.api_type,
        entity_name=args.entity_name,
    )

    print(f"Lignes apres concatenation/deduplication : {len(stg_df)}")

    if stg_df.empty:
        print("Aucune donnee API a charger dans BigQuery.")
        print("Verifie que ingest_api.py a bien depose du JSON sous le prefixe ci-dessus.")
        return

    load_to_bigquery(
        df=stg_df,
        project_id=args.gcp_project_id,
        dataset=args.dataset,
        table=args.table,
        write_mode=args.write_mode,
    )

    finished_at = datetime.now(timezone.utc)

    print("\n=== Resume ===")
    print(f"Objets traites : {len(object_names)}")
    print(f"Lignes chargees: {len(stg_df)}")
    print(f"Familles       : {sorted(stg_df['metric_family'].unique().tolist())}")
    print(f"Debut          : {started_at.isoformat()}")
    print(f"Fin            : {finished_at.isoformat()}")
    print("Statut         : success")


if __name__ == "__main__":
    main()
