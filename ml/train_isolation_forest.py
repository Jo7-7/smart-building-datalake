"""
Entrainement d'un modele Isolation Forest de detection d'anomalies energetiques.

Ce script lit la table Curated cur_energy_by_device (une ligne par appareil et
par horodatage, avec les mesures electriques en colonnes), entraine un modele
Isolation Forest par appareil sur ses propres features, puis sauvegarde
l'ensemble des modeles dans la zone Raw (MinIO) sous le prefixe models/.

Detecter par appareil permet de juger chaque machine contre son propre
comportement normal : un pic de 200 W est anormal pour une bouilloire au repos
mais routinier pour un frigo en degivrage.

Usage:
    python ml/train_isolation_forest.py \
        --dataset-curated curated \
        --table cur_energy_by_device \
        --gcp-project-id smart-building-datalake \
        --bucket raw \
        --model-prefix models \
        --contamination 0.02 \
        --minio-endpoint minio:9000 \
        --minio-access-key minio \
        --minio-secret-key minio123
"""

import argparse
import io
import pickle
from datetime import datetime, timezone

from google.cloud import bigquery
from minio import Minio
from sklearn.ensemble import IsolationForest


# Features electriques utilisees pour le scoring (memes colonnes que Curated).
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
        description="Entraine un Isolation Forest par appareil et sauvegarde les modeles dans MinIO."
    )
    parser.add_argument("--dataset-curated", type=str, required=True)
    parser.add_argument("--table", type=str, default="cur_energy_by_device")
    parser.add_argument("--gcp-project-id", type=str, required=True)
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--model-prefix", type=str, default="models")
    parser.add_argument("--contamination", type=float, default=0.02)
    parser.add_argument("--minio-endpoint", type=str, required=True)
    parser.add_argument("--minio-access-key", type=str, required=True)
    parser.add_argument("--minio-secret-key", type=str, required=True)
    parser.add_argument("--secure", action="store_true")
    return parser.parse_args()


def read_curated_features(client, project_id, dataset, table):
    """
    Charge la table de features Curated dans un DataFrame pandas.

    Steps
    -----
    1. Construire l'identifiant complet de la table
    2. Executer un SELECT * et materialiser le resultat
    3. Retourner le DataFrame pour entrainement local
    """
    table_id = f"{project_id}.{dataset}.{table}"
    query = f"SELECT * FROM `{table_id}`"
    return client.query(query).to_dataframe()


def train_one_model(device_df, contamination):
    """
    Entraine un Isolation Forest sur les features d'un seul appareil.

    Steps
    -----
    1. Selectionner les colonnes de features et remplir les valeurs manquantes
    2. Instancier un Isolation Forest avec le taux de contamination donne
    3. Ajuster le modele sur les features de l'appareil
    4. Retourner le modele entraine
    """
    features = device_df[FEATURE_COLUMNS].fillna(0.0)
    model = IsolationForest(
        contamination=contamination,
        random_state=42,
        n_estimators=100,
    )
    model.fit(features)
    return model


def save_model_to_minio(client, bucket, prefix, device, model):
    """
    Serialise un modele et le televerse dans MinIO sous models/<device>.pkl.

    Steps
    -----
    1. Serialiser le modele en memoire avec pickle
    2. Construire la cle objet a partir du nom d'appareil normalise
    3. Televerser l'objet dans le bucket Raw
    """
    payload = pickle.dumps(model)
    safe_device = device.replace(" ", "_")
    object_name = f"{prefix}/{safe_device}.pkl"
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/octet-stream",
    )
    return object_name


def main():
    """
    Point d'entree principal du script.

    Steps
    -----
    1. Lire la table de features Curated
    2. Pour chaque appareil, entrainer un Isolation Forest dedie
    3. Sauvegarder chaque modele dans MinIO
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

    print("=== Entrainement Isolation Forest (par appareil) ===")
    print(f"Table features : {args.dataset_curated}.{args.table}")
    print(f"Contamination  : {args.contamination}")
    print(f"Bucket modeles : {args.bucket}/{args.model_prefix}")

    df = read_curated_features(
        bq_client, args.gcp_project_id, args.dataset_curated, args.table
    )
    print(f"\nLignes de features lues : {len(df)}")

    devices = sorted(df["device"].unique().tolist())
    trained = []

    for device in devices:
        device_df = df[df["device"] == device]
        if len(device_df) < 10:
            print(f"[SKIP] {device} : trop peu de donnees ({len(device_df)} lignes)")
            continue
        model = train_one_model(device_df, args.contamination)
        object_name = save_model_to_minio(
            minio_client, args.bucket, args.model_prefix, device, model
        )
        trained.append((device, len(device_df), object_name))
        print(f"[OK] {device} : entraine sur {len(device_df)} lignes -> {object_name}")

    finished_at = datetime.now(timezone.utc)

    print("\n=== Resume ===")
    print(f"Appareils entraines : {len(trained)}")
    print(f"Debut               : {started_at.isoformat()}")
    print(f"Fin                 : {finished_at.isoformat()}")
    print("Statut              : success")


if __name__ == "__main__":
    main()