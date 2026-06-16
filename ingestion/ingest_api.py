"""
Ingestion d'une source API Open-Meteo vers la zone Raw (MinIO/S3).

Ce script appelle soit l'API qualité de l'air, soit l'API météo historique,
récupère un payload JSON brut, puis l'enregistre dans MinIO/S3
dans la zone Raw du Data Lake.

Usage:
    python ingestion/ingest_api.py \
        --bucket raw \
        --source-name open_meteo_api \
        --api-type weather \
        --latitude 25.3488 \
        --longitude 55.4054 \
        --start-date 2024-01-01 \
        --end-date 2024-06-20
"""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from io import BytesIO

import requests
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
    --bucket : nom du bucket MinIO/S3
    --source-name : nom logique de la source API
    --api-type : air_quality ou weather
    --latitude : latitude du point à interroger
    --longitude : longitude du point à interroger
    --start-date : date de début au format YYYY-MM-DD
    --end-date : date de fin au format YYYY-MM-DD
    --timezone : timezone Open-Meteo (ex: Asia/Dubai ou auto)
    --minio-endpoint : endpoint MinIO
    --minio-access-key : access key MinIO
    --minio-secret-key : secret key MinIO
    --secure : active HTTPS vers MinIO
    """
    parser = argparse.ArgumentParser(
        description="Ingère une source API Open-Meteo dans la zone Raw MinIO/S3."
    )
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--source-name", type=str, required=True)
    parser.add_argument("--api-type", type=str, choices=["air_quality", "weather"], required=True)
    parser.add_argument("--latitude", type=float, required=True)
    parser.add_argument("--longitude", type=float, required=True)
    parser.add_argument("--start-date", type=str, required=True)
    parser.add_argument("--end-date", type=str, required=True)
    parser.add_argument("--timezone", type=str, default="auto")
    parser.add_argument("--minio-endpoint", type=str, required=True)
    parser.add_argument("--minio-access-key", type=str, required=True)
    parser.add_argument("--minio-secret-key", type=str, required=True)
    parser.add_argument("--secure", action="store_true")
    return parser.parse_args()


def generate_run_id():
    """
    Génère un identifiant unique pour le run d'ingestion.

    Returns
    -------
    str
        Identifiant du run au format horodaté UTC.
    """
    return f"api_ingest_{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')}"


def build_air_quality_variables():
    """
    Construit la liste des variables hourly pour l'API qualité de l'air.

    Returns
    -------
    str
        Liste CSV des variables air quality.

    Notes
    -----
    L'Air Quality API accepte notamment pm2_5, pm10, carbon_monoxide,
    nitrogen_dioxide, ozone et european_aqi. [page:1]
    """
    variables = [
        "pm2_5",
        "pm10",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone",
        "european_aqi",
    ]
    return ",".join(variables)


def build_weather_variables():
    """
    Construit la liste des variables hourly pour l'API météo.

    Returns
    -------
    str
        Liste CSV des variables météo utiles pour un contexte IoT/énergie.

    Notes
    -----
    L'Historical Weather API documente notamment temperature_2m,
    relative_humidity_2m, surface_pressure, precipitation,
    wind_speed_10m, cloud_cover et shortwave_radiation. [page:2]
    """
    variables = [
        "temperature_2m",
        "relative_humidity_2m",
        "surface_pressure",
        "precipitation",
        "wind_speed_10m",
        "cloud_cover",
        "shortwave_radiation",
    ]
    return ",".join(variables)


def build_api_url(api_type, latitude, longitude, start_date, end_date, timezone_value):
    """
    Construit l'URL Open-Meteo à appeler selon le type d'API.

    Parameters
    ----------
    api_type : str
        air_quality ou weather.
    latitude : float
        Latitude du point.
    longitude : float
        Longitude du point.
    start_date : str
        Date de début.
    end_date : str
        Date de fin.
    timezone_value : str
        Timezone Open-Meteo.

    Returns
    -------
    str
        URL complète de l'API.
    """
    if api_type == "air_quality":
        hourly = build_air_quality_variables()
        return (
            "https://air-quality-api.open-meteo.com/v1/air-quality"
            f"?latitude={latitude}"
            f"&longitude={longitude}"
            f"&hourly={hourly}"
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            f"&timezone={timezone_value}"
        )

    hourly = build_weather_variables()
    return (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={latitude}"
        f"&longitude={longitude}"
        f"&hourly={hourly}"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
        f"&timezone={timezone_value}"
    )


def fetch_api_data(url):
    """
    Appelle l'API et retourne le payload JSON brut.

    Parameters
    ----------
    url : str
        URL de l'API à appeler.

    Returns
    -------
    dict
        Réponse JSON de l'API.

    Steps
    -----
    1. Exécuter une requête GET
    2. Vérifier que le statut HTTP est correct
    3. Retourner le JSON brut
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def compute_payload_sha256(payload):
    """
    Calcule le hash SHA256 du payload JSON.

    Parameters
    ----------
    payload : dict
        Payload JSON brut.

    Returns
    -------
    tuple[str, bytes]
        Hash SHA256 et version bytes du payload sérialisé.
    """
    payload_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()
    return payload_hash, payload_bytes


def build_raw_object_key(source_name, ingestion_date, api_type, run_id):
    """
    Construit la clé de stockage Raw pour le payload API.

    Parameters
    ----------
    source_name : str
        Nom logique de la source.
    ingestion_date : str
        Date du run au format YYYY-MM-DD.
    api_type : str
        air_quality ou weather.
    run_id : str
        Identifiant du run.

    Returns
    -------
    str
        Clé complète de l'objet JSON dans Raw.
    """
    return (
        f"source_api/"
        f"source_name={source_name}/"
        f"api_type={api_type}/"
        f"ingestion_date={ingestion_date}/"
        f"{run_id}.json"
    )


def upload_to_minio(payload_bytes, object_key, client, bucket):
    """
    Upload un payload JSON dans MinIO/S3.

    Parameters
    ----------
    payload_bytes : bytes
        Contenu JSON sérialisé.
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
    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=BytesIO(payload_bytes),
        length=len(payload_bytes),
        content_type="application/json",
    )


def build_manifest_entry(
    source_name,
    api_type,
    url,
    object_key,
    payload_hash,
    payload_bytes,
    status,
    latitude,
    longitude,
    start_date,
    end_date,
    timezone_value,
    error_message=None,
):
    """
    Construit l'entrée du manifest pour le payload API traité.

    Returns
    -------
    dict
        Entrée JSON du manifest.
    """
    payload_size = None
    if payload_bytes is not None:
        payload_size = len(payload_bytes)

    return {
        "source_name": source_name,
        "api_type": api_type,
        "request_url": url,
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": timezone_value,
        "raw_object_key": object_key,
        "payload_size_bytes": payload_size,
        "payload_hash_sha256": payload_hash,
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
    4. Construire l'URL de l'API
    5. Appeler l'API
    6. Sauvegarder la réponse brute dans Raw
    7. Écrire le manifest
    8. Afficher un résumé
    """
    args = parse_args()

    run_id = generate_run_id()
    started_at = datetime.now(timezone.utc)
    ingestion_date = started_at.strftime("%Y-%m-%d")

    client = Minio(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=args.secure,
    )

    if not client.bucket_exists(args.bucket):
        client.make_bucket(args.bucket)

    print("=== Ingestion API vers Raw ===")
    print(f"Source name : {args.source_name}")
    print(f"API type    : {args.api_type}")
    print(f"Bucket      : {args.bucket}")
    print(f"Latitude    : {args.latitude}")
    print(f"Longitude   : {args.longitude}")
    print(f"Start date  : {args.start_date}")
    print(f"End date    : {args.end_date}")
    print(f"Timezone    : {args.timezone}")
    print(f"Run id      : {run_id}")

    url = build_api_url(
        api_type=args.api_type,
        latitude=args.latitude,
        longitude=args.longitude,
        start_date=args.start_date,
        end_date=args.end_date,
        timezone_value=args.timezone,
    )

    payload_hash = None
    payload_bytes = None
    object_key = build_raw_object_key(
        source_name=args.source_name,
        ingestion_date=ingestion_date,
        api_type=args.api_type,
        run_id=run_id,
    )

    try:
        payload = fetch_api_data(url)
        payload_hash, payload_bytes = compute_payload_sha256(payload)

        upload_to_minio(
            payload_bytes=payload_bytes,
            object_key=object_key,
            client=client,
            bucket=args.bucket,
        )

        entry = build_manifest_entry(
            source_name=args.source_name,
            api_type=args.api_type,
            url=url,
            object_key=object_key,
            payload_hash=payload_hash,
            payload_bytes=payload_bytes,
            status="uploaded",
            latitude=args.latitude,
            longitude=args.longitude,
            start_date=args.start_date,
            end_date=args.end_date,
            timezone_value=args.timezone,
        )

        status = "success"
        print(f"[OK] Payload API sauvegardé -> {object_key}")

    except Exception as e:
        entry = build_manifest_entry(
            source_name=args.source_name,
            api_type=args.api_type,
            url=url,
            object_key=object_key,
            payload_hash=payload_hash,
            payload_bytes=payload_bytes,
            status="failed",
            latitude=args.latitude,
            longitude=args.longitude,
            start_date=args.start_date,
            end_date=args.end_date,
            timezone_value=args.timezone,
            error_message=str(e),
        )

        status = "failed"
        print(f"[ERREUR] Appel API : {e}")

    finished_at = datetime.now(timezone.utc)

    manifest = {
        "run_id": run_id,
        "pipeline_step": "ingest_api",
        "source_type": "api",
        "source_name": args.source_name,
        "api_type": args.api_type,
        "bucket": args.bucket,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": status,
        "request_url": url,
        "files": [entry],
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
    print(f"Statut du run : {status}")
    print(f"Objet Raw     : {object_key}")
    print(f"Manifest      : {manifest_key}")


if __name__ == "__main__":
    main()