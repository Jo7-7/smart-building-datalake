"""
Ingestion du dataset fichier vers la zone Raw (MinIO/S3).

Ce script parcourt un dossier local contenant des fichiers source,
calcule leurs métadonnées techniques, puis les envoie dans un bucket
MinIO/S3 représentant la zone Raw du Data Lake.

Usage:
    python ingestion/ingest_dataset.py \
        --dataset-root data \
        --bucket raw \
        --source-name smart_building_dataset
"""

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio


def parse_args():
    """
    Parse les arguments de ligne de commande.

    Returns
    -------
    argparse.Namespace
        Arguments fournis au script.

    Arguments attendus
    ------------------
    --dataset-root : chemin du dossier source local
    --bucket : nom du bucket MinIO/S3
    --source-name : nom logique de la source
    --minio-endpoint : endpoint MinIO
    --minio-access-key : access key MinIO
    --minio-secret-key : secret key MinIO
    --secure : active HTTPS vers MinIO
    --allowed-extensions : extensions autorisées, séparées par des virgules
    """
    parser = argparse.ArgumentParser(
        description="Ingère un dataset local dans la zone Raw MinIO/S3."
    )
    parser.add_argument("--dataset-root", type=str, required=True)
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--source-name", type=str, required=True)
    parser.add_argument("--minio-endpoint", type=str, required=True)
    parser.add_argument("--minio-access-key", type=str, required=True)
    parser.add_argument("--minio-secret-key", type=str, required=True)
    parser.add_argument("--secure", action="store_true")
    parser.add_argument(
        "--allowed-extensions",
        type=str,
        default=".csv,.json,.txt",
        help="Liste des extensions autorisées séparées par des virgules."
    )
    return parser.parse_args()


def generate_run_id():
    """
    Génère un identifiant unique pour le run d'ingestion.

    Returns
    -------
    str
        Identifiant du run au format horodaté UTC.
    """
    return f"dataset_ingest_{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')}"


def discover_files(dataset_root, allowed_extensions):
    """
    Parcourt récursivement le dossier dataset_root et retourne les fichiers autorisés.

    Parameters
    ----------
    dataset_root : str
        Chemin du dossier source.
    allowed_extensions : list[str]
        Liste des extensions autorisées.

    Returns
    -------
    list[Path]
        Liste triée des fichiers à ingérer.

    Steps
    -----
    1. Vérifier que le dossier existe
    2. Parcourir récursivement tous les fichiers
    3. Garder uniquement ceux dont l'extension est autorisée
    4. Retourner la liste triée
    """
    root = Path(dataset_root)

    if not root.exists():
        raise FileNotFoundError(f"Le dossier source n'existe pas : {dataset_root}")

    files = []
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in allowed_extensions:
            files.append(path)

    return sorted(files)


def compute_sha256(file_path):
    """
    Calcule le hash SHA256 d'un fichier.

    Parameters
    ----------
    file_path : Path
        Fichier à hasher.

    Returns
    -------
    str
        Hash SHA256 en hexadécimal.
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def normalize_metric_folder(folder_name):
    """
    Normalise le nom du dossier métier pour construire une clé Raw propre.

    Parameters
    ----------
    folder_name : str
        Nom du dossier parent du fichier.

    Returns
    -------
    str
        Version normalisée du nom.

    Exemples
    --------
    'Apparent power VA' -> 'apparent_power_va'
    'Current A' -> 'current_a'
    """
    value = folder_name.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def build_raw_object_key(source_name, ingestion_date, metric, file_name):
    """
    Construit la clé de stockage Raw dans MinIO/S3.

    Parameters
    ----------
    source_name : str
        Nom logique de la source.
    ingestion_date : str
        Date du run au format YYYY-MM-DD.
    metric : str
        Nom normalisé du dossier métier.
    file_name : str
        Nom original du fichier.

    Returns
    -------
    str
        Clé complète de l'objet dans Raw.
    """
    return (
        f"source_dataset/"
        f"source_name={source_name}/"
        f"ingestion_date={ingestion_date}/"
        f"metric={metric}/"
        f"{file_name}"
    )


def upload_to_minio(local_file_path, object_key, client, bucket):
    """
    Upload un fichier local dans MinIO/S3.

    Parameters
    ----------
    local_file_path : Path
        Fichier local à envoyer.
    object_key : str
        Clé cible dans le bucket.
    client : Minio
        Client MinIO initialisé.
    bucket : str
        Nom du bucket cible.

    Returns
    -------
    None
    """
    client.fput_object(
        bucket_name=bucket,
        object_name=object_key,
        file_path=str(local_file_path),
    )


def build_manifest_entry(
    local_file_path,
    dataset_root,
    source_name,
    ingestion_date,
    status,
    file_hash_sha256=None,
    error_message=None,
):
    """
    Construit une entrée du manifest pour un fichier traité.

    Parameters
    ----------
    local_file_path : Path
        Fichier local traité.
    dataset_root : Path
        Racine du dataset.
    source_name : str
        Nom logique de la source.
    ingestion_date : str
        Date du run.
    status : str
        Statut du traitement ('uploaded' ou 'failed').
    file_hash_sha256 : str | None
        Hash SHA256 déjà calculé si disponible.
    error_message : str | None
        Message d'erreur éventuel.

    Returns
    -------
    dict
        Entrée JSON du manifest.

    Notes
    -----
    Le hash n'est pas recalculé ici s'il a déjà été obtenu dans la boucle principale.
    Cela évite qu'une erreur de lecture du fichier provoque une seconde erreur
    pendant la construction du manifest.
    """
    metric_folder = local_file_path.parent.name
    metric = normalize_metric_folder(metric_folder)
    object_key = build_raw_object_key(
        source_name=source_name,
        ingestion_date=ingestion_date,
        metric=metric,
        file_name=local_file_path.name,
    )

    file_size = None
    try:
        file_size = local_file_path.stat().st_size
    except Exception:
        file_size = None

    relative_path = None
    try:
        relative_path = str(local_file_path.relative_to(dataset_root))
    except Exception:
        relative_path = str(local_file_path)

    return {
        "local_file_path": str(local_file_path),
        "relative_file_path": relative_path,
        "file_name": local_file_path.name,
        "file_extension": local_file_path.suffix.lower(),
        "file_size_bytes": file_size,
        "file_hash_sha256": file_hash_sha256,
        "metric_folder": metric_folder,
        "metric_normalized": metric,
        "raw_object_key": object_key,
        "status": status,
        "error_message": error_message,
    }


def write_manifest(client, bucket, source_name, ingestion_date, run_id, manifest):
    """
    Écrit le manifest JSON du run dans le bucket Raw.

    Parameters
    ----------
    client : Minio
        Client MinIO initialisé.
    bucket : str
        Bucket cible.
    source_name : str
        Nom logique de la source.
    ingestion_date : str
        Date du run.
    run_id : str
        Identifiant du run.
    manifest : dict
        Contenu du manifest.

    Returns
    -------
    str
        Clé du manifest dans MinIO/S3.
    """
    from io import BytesIO

    object_key = (
        f"manifests/"
        f"source_name={source_name}/"
        f"ingestion_date={ingestion_date}/"
        f"{run_id}.json"
    )

    payload = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")

    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    return object_key


def main():
    """
    Point d'entrée principal du script.

    Steps
    -----
    1. Lire les arguments CLI
    2. Initialiser le client MinIO
    3. Créer le bucket si nécessaire
    4. Découvrir les fichiers du dataset
    5. Uploader les fichiers un par un dans Raw
    6. Construire et écrire le manifest
    7. Afficher un résumé d'exécution
    """
    args = parse_args()

    allowed_extensions = [
        ext.strip().lower()
        for ext in args.allowed_extensions.split(",")
        if ext.strip()
    ]

    run_id = generate_run_id()
    started_at = datetime.now(timezone.utc)
    ingestion_date = started_at.strftime("%Y-%m-%d")
    dataset_root = Path(args.dataset_root)

    client = Minio(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=args.secure,
    )

    if not client.bucket_exists(args.bucket):
        client.make_bucket(args.bucket)

    print("=== Ingestion dataset vers Raw ===")
    print(f"Source name : {args.source_name}")
    print(f"Dataset root: {args.dataset_root}")
    print(f"Bucket      : {args.bucket}")
    print(f"Run id      : {run_id}")

    files = discover_files(args.dataset_root, allowed_extensions)
    print(f"\nFichiers découverts : {len(files)}")

    manifest_entries = []
    uploaded_count = 0
    failed_count = 0

    for file_path in files:
        file_hash = None

        try:
            file_hash = compute_sha256(file_path)

            metric = normalize_metric_folder(file_path.parent.name)
            object_key = build_raw_object_key(
                source_name=args.source_name,
                ingestion_date=ingestion_date,
                metric=metric,
                file_name=file_path.name,
            )

            upload_to_minio(
                local_file_path=file_path,
                object_key=object_key,
                client=client,
                bucket=args.bucket,
            )

            entry = build_manifest_entry(
                local_file_path=file_path,
                dataset_root=dataset_root,
                source_name=args.source_name,
                ingestion_date=ingestion_date,
                status="uploaded",
                file_hash_sha256=file_hash,
            )
            manifest_entries.append(entry)

            uploaded_count += 1
            print(f"[OK] {file_path.name} -> {object_key}")

        except Exception as e:
            entry = build_manifest_entry(
                local_file_path=file_path,
                dataset_root=dataset_root,
                source_name=args.source_name,
                ingestion_date=ingestion_date,
                status="failed",
                file_hash_sha256=file_hash,
                error_message=str(e),
            )
            manifest_entries.append(entry)

            failed_count += 1
            print(f"[ERREUR] {file_path} : {e}")

    finished_at = datetime.now(timezone.utc)

    status = "success"
    if failed_count > 0 and uploaded_count > 0:
        status = "partial_success"
    elif failed_count > 0 and uploaded_count == 0:
        status = "failed"

    manifest = {
        "run_id": run_id,
        "pipeline_step": "ingest_dataset",
        "source_type": "file_dataset",
        "source_name": args.source_name,
        "dataset_root": str(dataset_root),
        "bucket": args.bucket,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": status,
        "files_discovered": len(files),
        "files_uploaded": uploaded_count,
        "files_failed": failed_count,
        "files": manifest_entries,
    }

    manifest_key = write_manifest(
        client=client,
        bucket=args.bucket,
        source_name=args.source_name,
        ingestion_date=ingestion_date,
        run_id=run_id,
        manifest=manifest,
    )

    print("\n=== Résumé ===")
    print(f"Fichiers uploadés : {uploaded_count}")
    print(f"Fichiers en erreur: {failed_count}")
    print(f"Manifest          : {manifest_key}")
    print(f"Statut du run     : {status}")


if __name__ == "__main__":
    main()