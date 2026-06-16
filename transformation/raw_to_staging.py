"""
Chargement des fichiers CSV Raw depuis MinIO vers la table Staging BigQuery.

Ce script lit les objets CSV de la zone Raw stockés dans MinIO/S3,
les transforme vers un schéma Staging unifié de séries temporelles,
puis les charge dans BigQuery.

Usage:
    python transformation/raw_to_staging.py \
        --bucket raw \
        --dataset sb_staging \
        --table stg_sensor_timeseries \
        --source-name smart_building_dataset \
        --minio-endpoint localhost:9000 \
        --minio-access-key minioadmin \
        --minio-secret-key minioadmin \
        --gcp-project-id my-project \
        --write-mode WRITE_APPEND
"""

import argparse
import io
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from minio import Minio


METRIC_MAPPING = {
    "Apperent power VA": {
        "metric_family": "apparent_power_va",
        "metric_unit": "VA",
    },
    "Current A": {
        "metric_family": "current_a",
        "metric_unit": "A",
    },
    "Humidity": {
        "metric_family": "humidity_pct",
        "metric_unit": "%",
    },
    "Motion": {
        "metric_family": "motion_state",
        "metric_unit": "state",
    },
    "Temperature": {
        "metric_family": "temperature_c",
        "metric_unit": "°C",
    },
    "Voltage V": {
        "metric_family": "voltage_v",
        "metric_unit": "V",
    },
    "Watt W": {
        "metric_family": "power_w",
        "metric_unit": "W",
    },
    "Today": {
        "metric_family": "energy_kwh_today",
        "metric_unit": "kWh",
    },
    "Total": {
        "metric_family": "energy_kwh_total",
        "metric_unit": "kWh",
    },
}


def parse_args():
    """
    Parse les arguments de ligne de commande.
    """
    parser = argparse.ArgumentParser(
        description="Charge les CSV Raw MinIO vers une table Staging BigQuery."
    )
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--table", type=str, default="stg_sensor_timeseries")
    parser.add_argument("--source-name", type=str, required=True)
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


def build_raw_prefix(source_name):
    """
    Construit le préfixe Raw à explorer dans MinIO.
    """
    return f"source_dataset/source_name={source_name}/"


def list_raw_csv_objects(client, bucket, prefix):
    """
    Liste les objets CSV Raw correspondant au dataset source.
    """
    object_names = []

    for obj in client.list_objects(bucket_name=bucket, prefix=prefix, recursive=True):
        if obj.object_name.endswith(".csv"):
            object_names.append(obj.object_name)

    return sorted(object_names)


def extract_source_folder(object_name):
    """
    Extrait le dossier métier (label logique) depuis la clé objet Raw.
    """
    match = re.search(r"metric=([^/]+)/", object_name)
    if not match:
        raise ValueError(f"Impossible d'extraire metric depuis la clé : {object_name}")

    metric_normalized = match.group(1)

    reverse_mapping = {
        "apperent_power_va": "Apperent power VA",
        "apparent_power_va": "Apperent power VA",
        "current_a": "Current A",
        "humidity": "Humidity",
        "motion": "Motion",
        "temperature": "Temperature",
        "voltage_v": "Voltage V",
        "watt_w": "Watt W",
        "today": "Today",
        "total": "Total",
    }

    if metric_normalized not in reverse_mapping:
        raise ValueError(f"Metric inconnue dans la clé : {metric_normalized}")

    return reverse_mapping[metric_normalized]


def read_csv_from_minio(client, bucket, object_name):
    """
    Lit un CSV stocké dans MinIO et retourne un DataFrame pandas.
    """
    response = client.get_object(bucket_name=bucket, object_name=object_name)
    try:
        payload = response.read()
    finally:
        response.close()
        response.release_conn()

    return pd.read_csv(io.BytesIO(payload))


def extract_entity_name(file_name):
    """
    Extrait le nom logique de l'entité depuis le nom de fichier source.
    """
    name = Path(file_name).stem
    name = re.sub(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}\s+", "", name)
    name = re.sub(r"^Chronograf Data[- ]?", "", name, flags=re.IGNORECASE)
    return name.strip()


def transform_sensor_dataframe(df, source_folder, source_file_name):
    """
    Transforme un DataFrame brut vers le schéma Staging unifié.
    """
    if source_folder not in METRIC_MAPPING:
        raise ValueError(f"Dossier métier non supporté : {source_folder}")

    # Sélection de la colonne de valeur
    if source_folder in ("Today", "Total") and "kWh.mean_value" in df.columns:
        value_col = "kWh.mean_value"
    else:
        value_columns = [col for col in df.columns if col != "time"]
        if len(value_columns) != 1:
            raise ValueError(
                f"CSV inattendu pour {source_file_name}, colonnes mesure trouvées : {value_columns}"
            )
        value_col = value_columns[0]

    config = METRIC_MAPPING[source_folder]

    out = df.copy()
    out["event_time"] = pd.to_datetime(out["time"], errors="coerce", utc=True)
    out["metric_value"] = pd.to_numeric(out[value_col], errors="coerce")
    out["metric_family"] = config["metric_family"]
    out["metric_unit"] = config["metric_unit"]
    out["entity_name"] = extract_entity_name(source_file_name)
    out["source_file_name"] = source_file_name
    out["source_folder"] = source_folder
    out["ingested_at"] = pd.Timestamp.now("UTC")

    out = out[
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

    return out


def build_staging_dataframe(client, bucket, object_names):
    """
    Construit le DataFrame Staging global à partir des objets CSV Raw.
    """
    frames = []

    for object_name in object_names:
        source_file_name = Path(object_name).name

        try:
            source_folder = extract_source_folder(object_name)
            raw_df = read_csv_from_minio(client, bucket, object_name)
            stg_df = transform_sensor_dataframe(
                df=raw_df,
                source_folder=source_folder,
                source_file_name=source_file_name,
            )

            frames.append(stg_df)
            print(f"[OK] Transformé : {object_name} ({len(stg_df)} lignes)")

        except Exception as e:
            print(f"[WARN] Objet ignoré : {object_name} | raison : {e}")
            continue

    if not frames:
        return pd.DataFrame(
            columns=[
                "event_time",
                "metric_family",
                "metric_value",
                "metric_unit",
                "entity_name",
                "source_file_name",
                "source_folder",
                "ingested_at",
            ]
        )

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates()
    return df


def load_to_bigquery(df, project_id, dataset, table, write_mode):
    """
    Charge un DataFrame pandas dans BigQuery.
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
    Point d'entrée principal du script.
    """
    args = parse_args()

    started_at = datetime.now(timezone.utc)

    minio_client = Minio(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=args.secure,
    )

    prefix = build_raw_prefix(args.source_name)

    print("=== Raw vers Staging ===")
    print(f"Bucket source : {args.bucket}")
    print(f"Préfixe source: {prefix}")
    print(f"Dataset BQ    : {args.dataset}")
    print(f"Table BQ      : {args.table}")
    print(f"Write mode    : {args.write_mode}")

    object_names = list_raw_csv_objects(
        client=minio_client,
        bucket=args.bucket,
        prefix=prefix,
    )

    print(f"\nObjets CSV trouvés : {len(object_names)}")

    stg_df = build_staging_dataframe(
        client=minio_client,
        bucket=args.bucket,
        object_names=object_names,
    )

    print(f"Lignes après concaténation/déduplication : {len(stg_df)}")

    if stg_df.empty:
        print("Aucune donnée à charger dans BigQuery.")
        return

    load_to_bigquery(
        df=stg_df,
        project_id=args.gcp_project_id,
        dataset=args.dataset,
        table=args.table,
        write_mode=args.write_mode,
    )

    finished_at = datetime.now(timezone.utc)

    print("\n=== Résumé ===")
    print(f"Objets traités : {len(object_names)}")
    print(f"Lignes chargées: {len(stg_df)}")
    print(f"Début          : {started_at.isoformat()}")
    print(f"Fin            : {finished_at.isoformat()}")
    print("Statut         : success")


if __name__ == "__main__":
    main()