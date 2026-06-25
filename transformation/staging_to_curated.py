"""
Construction de la zone Curated BigQuery a partir de la table Staging longue.

Ce script lit la table Staging unifiee stg_sensor_timeseries (format long, une
ligne par mesure) et produit trois tables analytiques Curated, en respectant les
granularites reelles des donnees :

  1. cur_energy_by_device   : pivot large par (event_time, device), mesures
                              electriques. Sert de table de features au ML.
  2. cur_environment_by_room: pivot large par (event_time, room), temperature,
                              humidite, presence.
  3. cur_daily_summary      : agregats journaliers par entite, plus la qualite
                              d'air exterieure moyenne du jour.

Choix de conception :
  - L'entite agregee GENERAL (famille power_w, valeurs nulles) est EXCLUE de
    cur_energy_by_device, qui ne doit contenir que des appareils individuels.
  - Les variantes de pieces cote motion (Lab Motion, Lab Open colsed door) sont
    normalisees vers une piece canonique pour s'aligner avec temperature/humidite.
  - La qualite d'air (entity OUTDOOR, familles air_*) est un signal exterieur
    unique, rattache au niveau journalier plutot qu'a une entite interne.

Usage:
    python transformation/staging_to_curated.py \
        --dataset-staging staging \
        --dataset-curated curated \
        --staging-table stg_sensor_timeseries \
        --gcp-project-id smart-building-datalake \
        --write-mode WRITE_TRUNCATE
"""

import argparse
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery


# Familles electriques pivotees dans cur_energy_by_device (grain = appareil).
DEVICE_METRIC_FAMILIES = [
    "power_w",
    "apparent_power_va",
    "voltage_v",
    "current_a",
    "energy_kwh_today",
    "energy_kwh_total",
]

# Familles environnementales pivotees dans cur_environment_by_room (grain = piece).
ROOM_METRIC_FAMILIES = [
    "temperature_c",
    "humidity_pct",
    "motion_state",
]

# Familles de qualite d'air exterieure (grain = site, signal unique).
AIR_METRIC_FAMILIES = [
    "air_pm2_5",
    "air_pm10",
    "air_carbon_monoxide",
    "air_nitrogen_dioxide",
    "air_ozone",
    "air_european_aqi",
]

# Entite agregee du batiment, a exclure du grain appareil.
GENERAL_ENTITY = "GENERAL"


def parse_args():
    """
    Parse les arguments de ligne de commande.
    """
    parser = argparse.ArgumentParser(
        description="Construit la zone Curated BigQuery a partir de la Staging longue."
    )
    parser.add_argument("--dataset-staging", type=str, required=True)
    parser.add_argument("--dataset-curated", type=str, required=True)
    parser.add_argument("--staging-table", type=str, default="stg_sensor_timeseries")
    parser.add_argument("--gcp-project-id", type=str, required=True)
    parser.add_argument(
        "--write-mode",
        type=str,
        choices=["WRITE_APPEND", "WRITE_TRUNCATE"],
        default="WRITE_TRUNCATE",
    )
    return parser.parse_args()


def read_staging(client, project_id, dataset, table):
    """
    Charge l'integralite de la table Staging dans un DataFrame pandas.

    Steps
    -----
    1. Construire l'identifiant complet de la table Staging
    2. Executer un SELECT * et materialiser le resultat en DataFrame
    3. Retourner le DataFrame brut pour transformation locale
    """
    table_id = f"{project_id}.{dataset}.{table}"
    query = f"SELECT * FROM `{table_id}`"
    return client.query(query).to_dataframe()


def normalize_room(entity_name):
    """
    Normalise une entite environnementale vers une piece canonique.

    Les capteurs de presence portent des libelles comme 'Lab Motion' ou
    'Lab Open colsed door', alors que temperature et humidite portent 'Lab'.
    On ramene toutes ces variantes a la piece de base pour aligner les mesures.

    Steps
    -----
    1. Detecter le prefixe de piece connu dans le libelle
    2. Retourner la piece canonique correspondante
    3. A defaut, retourner le libelle d'origine nettoye
    """
    name = entity_name.strip()
    for room in ("Lab", "Kitchen", "Mailroom"):
        if name.lower().startswith(room.lower()):
            return room
    return name


def build_energy_by_device(staging_df):
    """
    Construit cur_energy_by_device par pivot des familles electriques.

    Steps
    -----
    1. Filtrer les familles electriques et exclure l'entite GENERAL
    2. Pivoter du format long vers le large sur (event_time, entity_name)
    3. Garantir la presence de toutes les colonnes de mesure attendues
    4. Renommer entity_name en device et trier le resultat
    """
    mask = (
        staging_df["metric_family"].isin(DEVICE_METRIC_FAMILIES)
        & (staging_df["entity_name"] != GENERAL_ENTITY)
    )
    subset = staging_df.loc[mask].copy()

    pivot = subset.pivot_table(
        index=["event_time", "entity_name"],
        columns="metric_family",
        values="metric_value",
        aggfunc="mean",
    ).reset_index()

    for family in DEVICE_METRIC_FAMILIES:
        if family not in pivot.columns:
            pivot[family] = pd.NA

    pivot = pivot.rename(columns={"entity_name": "device"})
    pivot.columns.name = None

    ordered = ["event_time", "device"] + DEVICE_METRIC_FAMILIES
    pivot = pivot[ordered].sort_values(["device", "event_time"]).reset_index(drop=True)
    return pivot


def build_environment_by_room(staging_df):
    """
    Construit cur_environment_by_room par pivot des familles environnementales.

    Steps
    -----
    1. Filtrer les familles environnementales
    2. Normaliser entity_name vers une piece canonique
    3. Pivoter du format long vers le large sur (event_time, room)
    4. Garantir les colonnes attendues, trier le resultat
    """
    subset = staging_df.loc[
        staging_df["metric_family"].isin(ROOM_METRIC_FAMILIES)
    ].copy()

    subset["room"] = subset["entity_name"].apply(normalize_room)

    pivot = subset.pivot_table(
        index=["event_time", "room"],
        columns="metric_family",
        values="metric_value",
        aggfunc="mean",
    ).reset_index()

    for family in ROOM_METRIC_FAMILIES:
        if family not in pivot.columns:
            pivot[family] = pd.NA

    pivot.columns.name = None

    ordered = ["event_time", "room"] + ROOM_METRIC_FAMILIES
    pivot = pivot[ordered].sort_values(["room", "event_time"]).reset_index(drop=True)
    return pivot


def build_daily_summary(energy_df, environment_df, staging_df):
    """
    Construit cur_daily_summary, agregats journaliers multi-grains.

    Steps
    -----
    1. Agreger la consommation par jour et par appareil
    2. Agreger l'environnement par jour et par piece
    3. Agreger la qualite d'air exterieure par jour (signal unique)
    4. Empiler les trois agregats dans un schema long commun
    """
    rows = []

    # 1. Energie : moyenne et max de puissance par jour et appareil
    energy = energy_df.copy()
    energy["day"] = pd.to_datetime(energy["event_time"]).dt.date
    energy_daily = (
        energy.groupby(["day", "device"])
        .agg(
            avg_power_w=("power_w", "mean"),
            max_power_w=("power_w", "max"),
            avg_current_a=("current_a", "mean"),
        )
        .reset_index()
    )
    for _, r in energy_daily.iterrows():
        rows.append(
            {
                "day": r["day"],
                "scope": "device",
                "entity": r["device"],
                "metric": "avg_power_w",
                "value": r["avg_power_w"],
            }
        )
        rows.append(
            {
                "day": r["day"],
                "scope": "device",
                "entity": r["device"],
                "metric": "max_power_w",
                "value": r["max_power_w"],
            }
        )

    # 2. Environnement : temperature et humidite moyennes par jour et piece
    env = environment_df.copy()
    env["day"] = pd.to_datetime(env["event_time"]).dt.date
    env_daily = (
        env.groupby(["day", "room"])
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            avg_humidity_pct=("humidity_pct", "mean"),
        )
        .reset_index()
    )
    for _, r in env_daily.iterrows():
        rows.append(
            {
                "day": r["day"],
                "scope": "room",
                "entity": r["room"],
                "metric": "avg_temperature_c",
                "value": r["avg_temperature_c"],
            }
        )
        rows.append(
            {
                "day": r["day"],
                "scope": "room",
                "entity": r["room"],
                "metric": "avg_humidity_pct",
                "value": r["avg_humidity_pct"],
            }
        )

    # 3. Qualite d'air : moyenne journaliere par famille air_*
    air = staging_df.loc[
        staging_df["metric_family"].isin(AIR_METRIC_FAMILIES)
    ].copy()
    if not air.empty:
        air["day"] = pd.to_datetime(air["event_time"]).dt.date
        air_daily = (
            air.groupby(["day", "metric_family"])["metric_value"]
            .mean()
            .reset_index()
        )
        for _, r in air_daily.iterrows():
            rows.append(
                {
                    "day": r["day"],
                    "scope": "outdoor",
                    "entity": "OUTDOOR",
                    "metric": r["metric_family"],
                    "value": r["metric_value"],
                }
            )

    summary = pd.DataFrame(
        rows, columns=["day", "scope", "entity", "metric", "value"]
    )
    summary["day"] = pd.to_datetime(summary["day"])
    summary = summary.sort_values(["day", "scope", "entity", "metric"]).reset_index(
        drop=True
    )
    return summary


def load_to_bigquery(client, df, project_id, dataset, table, write_mode):
    """
    Charge un DataFrame dans une table Curated BigQuery (schema auto-detecte).

    Steps
    -----
    1. Construire l'identifiant complet de la table cible
    2. Configurer le job de chargement avec le mode d'ecriture demande
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
    1. Lire la table Staging complete
    2. Construire les trois DataFrames Curated
    3. Charger chaque table dans le dataset Curated
    4. Recompter et afficher un resume de validation
    """
    args = parse_args()
    started_at = datetime.now(timezone.utc)

    client = bigquery.Client(project=args.gcp_project_id)

    print("=== Staging vers Curated ===")
    print(f"Dataset Staging : {args.dataset_staging}")
    print(f"Dataset Curated : {args.dataset_curated}")
    print(f"Table Staging   : {args.staging_table}")
    print(f"Write mode      : {args.write_mode}")

    staging_df = read_staging(
        client=client,
        project_id=args.gcp_project_id,
        dataset=args.dataset_staging,
        table=args.staging_table,
    )
    print(f"\nLignes Staging lues : {len(staging_df)}")

    energy_df = build_energy_by_device(staging_df)
    environment_df = build_environment_by_room(staging_df)
    summary_df = build_daily_summary(energy_df, environment_df, staging_df)

    load_to_bigquery(
        client, energy_df, args.gcp_project_id,
        args.dataset_curated, "cur_energy_by_device", args.write_mode,
    )
    load_to_bigquery(
        client, environment_df, args.gcp_project_id,
        args.dataset_curated, "cur_environment_by_room", args.write_mode,
    )
    load_to_bigquery(
        client, summary_df, args.gcp_project_id,
        args.dataset_curated, "cur_daily_summary", args.write_mode,
    )

    finished_at = datetime.now(timezone.utc)

    # Validation finale : recompte de ce qui a ete produit dans chaque table.
    print("\n=== Resume ===")
    print(f"cur_energy_by_device    : {len(energy_df)} lignes, "
          f"{energy_df['device'].nunique()} appareils")
    print(f"cur_environment_by_room : {len(environment_df)} lignes, "
          f"{environment_df['room'].nunique()} pieces")
    print(f"cur_daily_summary       : {len(summary_df)} lignes")
    print(f"Appareils               : {sorted(energy_df['device'].unique().tolist())}")
    print(f"Pieces                  : {sorted(environment_df['room'].unique().tolist())}")
    print(f"Debut                   : {started_at.isoformat()}")
    print(f"Fin                     : {finished_at.isoformat()}")
    print("Statut                  : success")


if __name__ == "__main__":
    main()