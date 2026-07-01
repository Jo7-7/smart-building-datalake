"""
Application des modeles Isolation Forest et ecriture des scores d'anomalies.

Ce script charge les modeles entraines depuis MinIO (un par appareil), score
chaque ligne de cur_energy_by_device, puis ecrit le resultat dans la table
Curated cur_anomaly_scores (event_time, device, features, if_score, if_is_anomaly).

Un score negatif indique une observation atypique ; le drapeau if_is_anomaly
vaut True quand le modele classe la ligne comme anomalie (predict == -1).

Usage:
    python ml/apply_models.py \
        --dataset-curated curated \
        --features-table cur_energy_by_device \
        --scores-table cur_anomaly_scores \
        --gcp-project-id smart-building-datalake \
        --bucket raw \
        --model-prefix models \
        --minio-endpoint minio:9000 \
        --minio-access-key minio \
        --minio-secret-key minio123 \
        --write-mode WRITE_TRUNCATE
"""

import argparse
import pickle
from datetime import datetime, timezone

from google.cloud import bigquery
from minio import Minio
from minio.error import S3Error


# Memes features que celles utilisees a l'entrainement.
FEATURE_COLUMNS = [
    "power_w",
    "apparent_power_va",
    "voltage_v",
    "current_a",
    "energy_kwh_today",
    "energy_kwh_total",
]


def parse_args():
    """
    Parse les arguments de ligne de commande.
    """
    parser = argparse.ArgumentParser(
        description="Applique les Isolation Forest et ecrit cur_anomaly_scores."
    )
    parser.add_argument("--dataset-curated", type=str, required=True)
    parser.add_argument("--features-table", type=str, default="cur_energy_by_device")
    parser.add_argument("--scores-table", type=str, default="cur_anomaly_scores")
    parser.add_argument("--gcp-project-id", type=str, required=True)
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--model-prefix", type=str, default="models")
    parser.add_argument("--minio-endpoint", type=str, required=True)
    parser.add_argument("--minio-access-key", type=str, required=True)
    parser.add_argument("--minio-secret-key", type=str, required=True)
    parser.add_argument("--secure", action="store_true")
    parser.add_argument(
        "--write-mode",
        type=str,
        choices=["WRITE_APPEND", "WRITE_TRUNCATE"],
        default="WRITE_TRUNCATE",
    )
    return parser.parse_args()


def read_features(client, project_id, dataset, table):
    """
    Charge la table de features Curated dans un DataFrame pandas.

    Steps
    -----
    1. Construire l'identifiant complet de la table
    2. Executer un SELECT * et materialiser le resultat
    3. Retourner le DataFrame
    """
    table_id = f"{project_id}.{dataset}.{table}"
    query = f"SELECT * FROM `{table_id}`"
    return client.query(query).to_dataframe()


def load_model_from_minio(client, bucket, prefix, device):
    """
    Charge le modele d'un appareil depuis MinIO, s'il existe.

    Steps
    -----
    1. Construire la cle objet a partir du nom d'appareil normalise
    2. Telecharger l'objet et le deserialiser avec pickle
    3. Retourner le modele, ou None si absent
    """
    safe_device = device.replace(" ", "_")
    object_name = f"{prefix}/{safe_device}.pkl"
    try:
        response = client.get_object(bucket, object_name)
        model = pickle.loads(response.read())
        return model
    except S3Error:
        return None


def score_devices(features_df, minio_client, bucket, prefix):
    """
    Score chaque appareil avec son modele et assemble les resultats.

    Steps
    -----
    1. Pour chaque appareil, charger son modele depuis MinIO
    2. Calculer le score de decision et la prediction (-1 = anomalie)
    3. Construire les colonnes if_score et if_is_anomaly
    4. Concatener tous les appareils dans un DataFrame de scores
    """
    import pandas as pd

    scored_parts = []
    summary = []

    for device in sorted(features_df["device"].unique().tolist()):
        device_df = features_df[features_df["device"] == device].copy()
        model = load_model_from_minio(minio_client, bucket, prefix, device)
        if model is None:
            print(f"[SKIP] {device} : aucun modele trouve")
            continue

        features = device_df[FEATURE_COLUMNS].fillna(0.0)
        device_df["if_score"] = model.decision_function(features)
        device_df["if_is_anomaly"] = model.predict(features) == -1

        keep = ["event_time", "device"] + FEATURE_COLUMNS + ["if_score", "if_is_anomaly"]
        scored_parts.append(device_df[keep])

        n_anom = int(device_df["if_is_anomaly"].sum())
        summary.append((device, len(device_df), n_anom))
        print(f"[OK] {device} : {len(device_df)} lignes, {n_anom} anomalies")

    scores_df = pd.concat(scored_parts, ignore_index=True) if scored_parts else pd.DataFrame()
    return scores_df, summary


def load_to_bigquery(client, df, project_id, dataset, table, write_mode):
    """
    Charge le DataFrame de scores dans la table Curated cible.

    Steps
    -----
    1. Construire l'identifiant complet de la table
    2. Configurer le job avec le mode d'ecriture demande
    3. Lancer le chargement et attendre sa fin
    """
    table_id = f"{project_id}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def main():
    """
    Point d'entree principal du script.

    Steps
    -----
    1. Lire la table de features Curated
    2. Charger les modeles et scorer chaque appareil
    3. Ecrire les scores dans cur_anomaly_scores
    4. Afficher un resume de validation
    """
    args = parse_args()
    started_at = datetime.now(timezone.utc)

    bq_client = bigquery.Client(project=args.gcp_project_id)
    minio_client = Minio(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=args.secure,
    )

    print("=== Application Isolation Forest -> cur_anomaly_scores ===")
    print(f"Features : {args.dataset_curated}.{args.features_table}")
    print(f"Scores   : {args.dataset_curated}.{args.scores_table}")
    print(f"Modeles  : {args.bucket}/{args.model_prefix}")
    print(f"Write mode: {args.write_mode}")

    features_df = read_features(
        bq_client, args.gcp_project_id, args.dataset_curated, args.features_table
    )
    print(f"\nLignes de features lues : {len(features_df)}")

    scores_df, summary = score_devices(
        features_df, minio_client, args.bucket, args.model_prefix
    )

    if scores_df.empty:
        print("\nAucun score produit (modeles introuvables). Lancez l'entrainement d'abord.")
        return

    load_to_bigquery(
        bq_client, scores_df, args.gcp_project_id,
        args.dataset_curated, args.scores_table, args.write_mode,
    )

    finished_at = datetime.now(timezone.utc)
    total_anom = sum(n for _, _, n in summary)

    print("\n=== Resume ===")
    print(f"Lignes scorees     : {len(scores_df)}")
    print(f"Anomalies detectees: {total_anom}")
    print(f"Appareils traites  : {len(summary)}")
    print(f"Debut              : {started_at.isoformat()}")
    print(f"Fin                : {finished_at.isoformat()}")
    print("Statut             : success")


if __name__ == "__main__":
    main()