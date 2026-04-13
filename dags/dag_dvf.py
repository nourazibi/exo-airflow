"""
DAG : Pipeline DVF — Data Lake (HDFS) vers Data Warehouse (PostgreSQL)
Avec bonus :
- partitionnement HDFS par annee/dept
- analyse de tendance mois sur mois
"""

from __future__ import annotations

import io
import logging
import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DVF_URL = "https://www.data.gouv.fr/fr/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _webhdfs_url(path: str, op: str, **params) -> str:
    path = path if path.startswith("/") else f"/{path}"
    query_params = {"op": op, "user.name": WEBHDFS_USER, **params}
    return f"{WEBHDFS_BASE_URL}{path}?{urlencode(query_params)}"


def _safe_float(value):
    if pd.isna(value):
        return None
    return float(value)


def _safe_int(value):
    if pd.isna(value):
        return None
    return int(value)


def _upload_file_to_hdfs(local_file_path: str, hdfs_file_path: str) -> str:
    init_url = _webhdfs_url(hdfs_file_path, "CREATE", overwrite="true")
    init_resp = requests.put(init_url, allow_redirects=False, timeout=30)

    if init_resp.status_code not in (307, 201):
        raise AirflowException(
            f"Échec initialisation upload WebHDFS: {init_resp.status_code} - {init_resp.text}"
        )

    redirect_url = init_resp.headers.get("Location")

    if init_resp.status_code == 307:
        if not redirect_url:
            raise AirflowException("Aucune URL de redirection WebHDFS reçue.")

        with open(local_file_path, "rb") as f:
            upload_resp = requests.put(
                redirect_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=300,
            )

        if upload_resp.status_code != 201:
            raise AirflowException(
                f"Échec upload vers DataNode: {upload_resp.status_code} - {upload_resp.text}"
            )

    return hdfs_file_path


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement -> HDFS raw/partitions -> PostgreSQL curated",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql", "bonus"],
)
def pipeline_dvf():

    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        statuts = {}

        try:
            resp = requests.get(DVF_URL, stream=True, timeout=20, allow_redirects=True)
            statuts["dvf_api"] = resp.status_code == 200
            resp.close()
        except Exception:
            logger.exception("API DVF inaccessible")
            statuts["dvf_api"] = False

        try:
            hdfs_url = _webhdfs_url("/", "LISTSTATUS")
            resp = requests.get(hdfs_url, timeout=10)
            statuts["hdfs"] = resp.status_code < 400
        except Exception:
            logger.exception("HDFS inaccessible")
            statuts["hdfs"] = False

        logger.info("Statut API DVF: %s", statuts["dvf_api"])
        logger.info("Statut HDFS: %s", statuts["hdfs"])

        if not statuts["hdfs"]:
            raise AirflowException(f"HDFS inaccessible: {statuts}")

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        local_path = os.path.join(tempfile.gettempdir(), "dvf_source.zip")
        total_bytes = 0
        seuil_log = 50 * 1024 * 1024
        prochain_log = seuil_log

        try:
            with requests.get(DVF_URL, stream=True, timeout=300, allow_redirects=True) as resp:
                resp.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            total_bytes += len(chunk)
                            if total_bytes >= prochain_log:
                                logger.info("Téléchargés: %.2f Mo", total_bytes / (1024 * 1024))
                                prochain_log += seuil_log
        except Exception as e:
            raise AirflowException(f"Échec téléchargement DVF : {e}")

        if not os.path.exists(local_path) or os.path.getsize(local_path) < 1000:
            raise AirflowException("Le fichier DVF téléchargé est vide ou invalide.")

        logger.info("Fichier téléchargé : %s", local_path)
        return local_path

    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        hdfs_file_path = f"{HDFS_RAW_PATH}/dvf_source.zip"

        mkdirs_url = _webhdfs_url(f"{HDFS_RAW_PATH}/", "MKDIRS")
        mkdirs_resp = requests.put(mkdirs_url, timeout=30)
        mkdirs_resp.raise_for_status()

        mkdirs_json = mkdirs_resp.json()
        if not mkdirs_json.get("boolean", False):
            raise AirflowException(f"Échec création répertoire HDFS: {mkdirs_json}")

        result = _upload_file_to_hdfs(local_path, hdfs_file_path)
        logger.info("Fichier raw stocké dans HDFS: %s", result)
        return result

    @task(task_id="partitionner_et_stocker_hdfs")
    def partitionner_et_stocker_hdfs(local_path: str) -> list[str]:
        temp_dir = tempfile.mkdtemp(prefix="dvf_partitions_")
        uploaded_paths = []

        rename_map = {
            "commune": "nom_commune",
            "nom_commune": "nom_commune",
            "code_postal": "code_postal",
            "type_local": "type_local",
            "nature_mutation": "nature_mutation",
            "date_mutation": "date_mutation",
            "valeur_fonciere": "valeur_fonciere",
            "surface_reelle_bati": "surface_reelle_bati",
            "nombre_de_pieces_principales": "nombre_pieces_principales",
            "nombre_pieces_principales": "nombre_pieces_principales",
            "adresse_numero": "adresse_numero",
            "adresse_nom_voie": "adresse_nom_voie",
            "code_departement": "code_departement",
            "code_commune": "code_commune",
            "longitude": "longitude",
            "latitude": "latitude",
        }

        local_partition_files = {}

        with zipfile.ZipFile(local_path, "r") as zf:
            txt_files = [name for name in zf.namelist() if name.lower().endswith(".txt")]
            if not txt_files:
                raise AirflowException("Aucun fichier .txt trouvé dans le ZIP DVF.")

            txt_name = txt_files[0]
            logger.info("Fichier texte trouvé pour partitionnement : %s", txt_name)

            with zf.open(txt_name) as txt_file:
                chunks = pd.read_csv(
                    txt_file,
                    sep="|",
                    dtype=str,
                    encoding="latin-1",
                    engine="python",
                    on_bad_lines="skip",
                    chunksize=100000,
                )

                for chunk in chunks:
                    chunk.columns = [
                        col.strip().lower().replace(" ", "_").replace("-", "_")
                        for col in chunk.columns
                    ]
                    chunk = chunk.rename(columns={c: rename_map[c] for c in chunk.columns if c in rename_map})

                    if "code_postal" not in chunk.columns or "date_mutation" not in chunk.columns:
                        continue

                    chunk["code_postal"] = (
                        chunk["code_postal"]
                        .astype(str)
                        .str.strip()
                        .str.replace(".0", "", regex=False)
                    )
                    chunk["code_departement"] = chunk["code_postal"].str[:2]

                    date_series = chunk["date_mutation"].astype(str).str.strip()
                    parsed_dates = pd.to_datetime(date_series, errors="coerce")

                    mask_na = parsed_dates.isna()
                    if mask_na.any():
                        parsed_dates.loc[mask_na] = pd.to_datetime(
                            date_series.loc[mask_na],
                            dayfirst=True,
                            errors="coerce",
                        )

                    chunk["date_mutation"] = parsed_dates
                    chunk["annee_mutation"] = chunk["date_mutation"].dt.year

                    chunk = chunk[
                        chunk["code_departement"].notna()
                        & chunk["annee_mutation"].notna()
                    ].copy()

                    for (dept, annee), part_df in chunk.groupby(["code_departement", "annee_mutation"]):
                        filename = f"dvf_{dept}_{int(annee)}.csv"
                        local_csv_path = os.path.join(temp_dir, filename)

                        write_header = not os.path.exists(local_csv_path)
                        part_df.to_csv(
                            local_csv_path,
                            sep="|",
                            index=False,
                            mode="a",
                            header=write_header,
                        )
                        local_partition_files[(dept, int(annee))] = local_csv_path

        for (dept, annee), local_csv_path in local_partition_files.items():
            part_dir = f"{HDFS_RAW_PATH}/annee={annee}/dept={dept}"
            hdfs_path = f"{part_dir}/dvf_{dept}_{annee}.csv"

            mkdirs_url = _webhdfs_url(f"{part_dir}/", "MKDIRS")
            mkdirs_resp = requests.put(mkdirs_url, timeout=30)
            mkdirs_resp.raise_for_status()

            _upload_file_to_hdfs(local_csv_path, hdfs_path)
            uploaded_paths.append(hdfs_path)

        shutil.rmtree(temp_dir, ignore_errors=True)

        logger.info("Partitions HDFS créées : %s", len(uploaded_paths))
        return uploaded_paths

    @task(task_id="traiter_donnees")
    def traiter_donnees(partition_paths: list[str]) -> dict:
        paths_75 = [p for p in partition_paths if "/dept=75/" in p]
        if not paths_75:
            raise AirflowException("Aucune partition dept=75 trouvée dans HDFS.")

        def extract_year(path: str) -> int:
            marker = "annee="
            if marker in path:
                return int(path.split(marker)[1].split("/")[0])
            return 0

        selected_path = max(paths_75, key=extract_year)
        logger.info("Partition retenue pour traitement : %s", selected_path)

        open_url = _webhdfs_url(selected_path, "OPEN")
        resp = requests.get(open_url, allow_redirects=True, timeout=300)
        resp.raise_for_status()

        csv_bytes = resp.content
        if not csv_bytes:
            raise AirflowException("La partition HDFS lue est vide.")

        df = pd.read_csv(
            io.BytesIO(csv_bytes),
            sep="|",
            dtype=str,
            encoding="latin-1",
            engine="python",
            on_bad_lines="skip",
        )

        df.columns = [
            col.strip().lower().replace(" ", "_").replace("-", "_")
            for col in df.columns
        ]

        rename_map = {
            "commune": "nom_commune",
            "nom_commune": "nom_commune",
            "code_postal": "code_postal",
            "type_local": "type_local",
            "nature_mutation": "nature_mutation",
            "date_mutation": "date_mutation",
            "valeur_fonciere": "valeur_fonciere",
            "surface_reelle_bati": "surface_reelle_bati",
            "nombre_de_pieces_principales": "nombre_pieces_principales",
            "nombre_pieces_principales": "nombre_pieces_principales",
            "adresse_numero": "adresse_numero",
            "adresse_nom_voie": "adresse_nom_voie",
            "code_departement": "code_departement",
            "code_commune": "code_commune",
            "longitude": "longitude",
            "latitude": "latitude",
        }
        df = df.rename(columns={c: rename_map[c] for c in df.columns if c in rename_map})

        required_cols = [
            "date_mutation",
            "nature_mutation",
            "valeur_fonciere",
            "code_postal",
            "nom_commune",
            "type_local",
            "surface_reelle_bati",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise AirflowException(f"Colonnes manquantes dans la partition DVF: {missing}")

        nb_avant = len(df)

        for col in ["valeur_fonciere", "surface_reelle_bati", "nombre_pieces_principales", "longitude", "latitude"]:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.replace(" ", "", regex=False)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["code_postal"] = (
            df["code_postal"]
            .astype(str)
            .str.strip()
            .str.replace(".0", "", regex=False)
        )

        date_series = df["date_mutation"].astype(str).str.strip()
        parsed_dates = pd.to_datetime(date_series, errors="coerce")

        mask_na = parsed_dates.isna()
        if mask_na.any():
            parsed_dates.loc[mask_na] = pd.to_datetime(
                date_series.loc[mask_na],
                dayfirst=True,
                errors="coerce",
            )

        df["date_mutation"] = parsed_dates

        cp_paris = [f"750{i:02d}" for i in range(1, 21)] + ["75116"]

        df = df[
            (df["type_local"] == "Appartement")
            & (df["nature_mutation"] == "Vente")
            & (df["code_postal"].isin(cp_paris))
            & (df["surface_reelle_bati"].between(9, 500, inclusive="both"))
            & (df["valeur_fonciere"] > 10000)
        ].copy()

        df = df[df["surface_reelle_bati"] > 0].copy()
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        def extraire_arrondissement(code_postal: str):
            cp = str(code_postal)
            if cp == "75116":
                return 16
            if cp.startswith("750") and len(cp) == 5:
                return int(cp[-2:])
            return None

        df["arrondissement"] = df["code_postal"].apply(extraire_arrondissement)
        df = df[df["arrondissement"].notna()].copy()
        df["arrondissement"] = df["arrondissement"].astype(int)

        nb_apres = len(df)
        logger.info("Nombre de lignes avant filtrage : %s", nb_avant)
        logger.info("Nombre de lignes après filtrage : %s", nb_apres)

        if nb_apres == 0:
            raise AirflowException("Aucune ligne restante après filtrage.")

        df = df[df["date_mutation"].notna()].copy()

        df["annee_mutation"] = df["date_mutation"].dt.year
        df["mois_mutation"] = df["date_mutation"].dt.month

        df = df[
            df["annee_mutation"].notna()
            & df["mois_mutation"].notna()
        ].copy()

        if df.empty:
            raise AirflowException(
                "Les lignes filtrées existent, mais aucune date_mutation valide n'a pu être convertie."
            )

        periode_ref = (
            df.groupby(["annee_mutation", "mois_mutation"])
            .size()
            .reset_index(name="nb")
            .sort_values("nb", ascending=False)
        )

        if periode_ref.empty:
            raise AirflowException("Impossible de déterminer la période de référence.")

        ref_annee = int(periode_ref.iloc[0]["annee_mutation"])
        ref_mois = int(periode_ref.iloc[0]["mois_mutation"])

        df_ref = df[
            (df["annee_mutation"] == ref_annee)
            & (df["mois_mutation"] == ref_mois)
        ].copy()

        if df_ref.empty:
            raise AirflowException("Aucune donnée trouvée pour la période de référence.")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run(
            "DELETE FROM dvf_raw WHERE annee_mutation = %s AND mois_mutation = %s",
            parameters=(ref_annee, ref_mois),
        )

        raw_fields = [
            "date_mutation",
            "nature_mutation",
            "valeur_fonciere",
            "adresse_numero",
            "adresse_nom_voie",
            "code_postal",
            "nom_commune",
            "code_departement",
            "code_commune",
            "type_local",
            "surface_reelle_bati",
            "nombre_pieces_principales",
            "longitude",
            "latitude",
            "prix_m2",
            "annee_mutation",
            "mois_mutation",
        ]

        for field in raw_fields:
            if field not in df_ref.columns:
                df_ref[field] = None

        rows_raw = []
        for _, row in df_ref[raw_fields].iterrows():
            rows_raw.append(
                (
                    None if pd.isna(row["date_mutation"]) else row["date_mutation"].date(),
                    None if pd.isna(row["nature_mutation"]) else str(row["nature_mutation"]),
                    _safe_float(row["valeur_fonciere"]),
                    None if pd.isna(row["adresse_numero"]) else str(row["adresse_numero"]),
                    None if pd.isna(row["adresse_nom_voie"]) else str(row["adresse_nom_voie"]),
                    None if pd.isna(row["code_postal"]) else str(row["code_postal"]),
                    None if pd.isna(row["nom_commune"]) else str(row["nom_commune"]),
                    None if pd.isna(row["code_departement"]) else str(row["code_departement"]),
                    None if pd.isna(row["code_commune"]) else str(row["code_commune"]),
                    None if pd.isna(row["type_local"]) else str(row["type_local"]),
                    _safe_float(row["surface_reelle_bati"]),
                    _safe_int(row["nombre_pieces_principales"]),
                    _safe_float(row["longitude"]),
                    _safe_float(row["latitude"]),
                    _safe_float(row["prix_m2"]),
                    _safe_int(row["annee_mutation"]),
                    _safe_int(row["mois_mutation"]),
                )
            )

        if rows_raw:
            hook.insert_rows(
                table="dvf_raw",
                rows=rows_raw,
                target_fields=raw_fields,
                commit_every=1000,
                replace=False,
            )

        grouped = (
            df_ref.groupby(["code_postal", "arrondissement"], as_index=False)
            .agg(
                prix_m2_moyen=("prix_m2", "mean"),
                prix_m2_median=("prix_m2", "median"),
                prix_m2_min=("prix_m2", "min"),
                prix_m2_max=("prix_m2", "max"),
                nb_transactions=("prix_m2", "count"),
                surface_moyenne=("surface_reelle_bati", "mean"),
            )
            .sort_values("arrondissement")
        )

        agregats = []
        for _, row in grouped.iterrows():
            agregats.append(
                {
                    "code_postal": str(row["code_postal"]),
                    "arrondissement": int(row["arrondissement"]),
                    "annee": ref_annee,
                    "mois": ref_mois,
                    "prix_m2_moyen": round(float(row["prix_m2_moyen"]), 2),
                    "prix_m2_median": round(float(row["prix_m2_median"]), 2),
                    "prix_m2_min": round(float(row["prix_m2_min"]), 2),
                    "prix_m2_max": round(float(row["prix_m2_max"]), 2),
                    "nb_transactions": int(row["nb_transactions"]),
                    "surface_moyenne": round(float(row["surface_moyenne"]), 2),
                }
            )

        idx_max = grouped["prix_m2_median"].idxmax()
        idx_min = grouped["prix_m2_median"].idxmin()

        stats_globales = {
            "annee": ref_annee,
            "mois": ref_mois,
            "nb_transactions_total": int(len(df_ref)),
            "prix_m2_median_paris": round(float(df_ref["prix_m2"].median()), 2),
            "prix_m2_moyen_paris": round(float(df_ref["prix_m2"].mean()), 2),
            "arrdt_plus_cher": int(grouped.loc[idx_max, "arrondissement"]),
            "arrdt_moins_cher": int(grouped.loc[idx_min, "arrondissement"]),
            "surface_mediane": round(float(df_ref["surface_reelle_bati"].median()), 2),
        }

        return {"agregats": agregats, "stats_globales": stats_globales}

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})

        upsert_query = """
        INSERT INTO prix_m2_arrondissement
        (
            code_postal, arrondissement, annee, mois,
            prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
            nb_transactions, surface_moyenne, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
            prix_m2_moyen = EXCLUDED.prix_m2_moyen,
            prix_m2_median = EXCLUDED.prix_m2_median,
            prix_m2_min = EXCLUDED.prix_m2_min,
            prix_m2_max = EXCLUDED.prix_m2_max,
            nb_transactions = EXCLUDED.nb_transactions,
            surface_moyenne = EXCLUDED.surface_moyenne,
            updated_at = NOW();
        """

        nb_lignes = 0
        for agg in agregats:
            hook.run(
                upsert_query,
                parameters=(
                    agg["code_postal"],
                    agg["arrondissement"],
                    agg["annee"],
                    agg["mois"],
                    agg["prix_m2_moyen"],
                    agg["prix_m2_median"],
                    agg["prix_m2_min"],
                    agg["prix_m2_max"],
                    agg["nb_transactions"],
                    agg["surface_moyenne"],
                ),
            )
            nb_lignes += 1

        upsert_stats = """
        INSERT INTO stats_marche
        (
            annee, mois, nb_transactions_total,
            prix_m2_median_paris, prix_m2_moyen_paris,
            arrdt_plus_cher, arrdt_moins_cher,
            surface_mediane, variation_median_pct, date_calcul
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, NOW())
        ON CONFLICT (annee, mois) DO UPDATE SET
            nb_transactions_total = EXCLUDED.nb_transactions_total,
            prix_m2_median_paris = EXCLUDED.prix_m2_median_paris,
            prix_m2_moyen_paris = EXCLUDED.prix_m2_moyen_paris,
            arrdt_plus_cher = EXCLUDED.arrdt_plus_cher,
            arrdt_moins_cher = EXCLUDED.arrdt_moins_cher,
            surface_mediane = EXCLUDED.surface_mediane,
            date_calcul = NOW();
        """

        if stats_globales:
            hook.run(
                upsert_stats,
                parameters=(
                    stats_globales["annee"],
                    stats_globales["mois"],
                    stats_globales["nb_transactions_total"],
                    stats_globales["prix_m2_median_paris"],
                    stats_globales["prix_m2_moyen_paris"],
                    stats_globales["arrdt_plus_cher"],
                    stats_globales["arrdt_moins_cher"],
                    stats_globales["surface_mediane"],
                ),
            )

        logger.info("Nombre de lignes insérées / mises à jour : %s", nb_lignes)
        return nb_lignes

    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        latest_period = hook.get_first(
            """
            SELECT annee, mois
            FROM prix_m2_arrondissement
            ORDER BY annee DESC, mois DESC
            LIMIT 1
            """
        )

        if not latest_period:
            return "Aucune donnée disponible pour générer le rapport."

        annee, mois = latest_period

        query = """
        SELECT
            arrondissement,
            prix_m2_median,
            prix_m2_moyen,
            nb_transactions,
            surface_moyenne
        FROM prix_m2_arrondissement
        WHERE annee = %s AND mois = %s
        ORDER BY prix_m2_median DESC
        LIMIT 20;
        """

        records = hook.get_records(query, parameters=(annee, mois))

        lines = []
        lines.append(f"Rapport DVF Paris - {mois:02d}/{annee}")
        lines.append("")
        lines.append("Arrondissement | Median (EUR/m2) | Moyen (EUR/m2) | Transactions | Surface moy.")
        lines.append("-------------- | --------------- | -------------- | ------------ | ------------")

        for row in records:
            arrondissement, mediane, moyenne, nb_tx, surface = row
            lines.append(
                f"{arrondissement:>13} | "
                f"{float(mediane):>15.2f} | "
                f"{float(moyenne):>14.2f} | "
                f"{int(nb_tx):>12} | "
                f"{float(surface):>12.2f}"
            )

        rapport = "\n".join(lines)
        logger.info("\n%s", rapport)
        logger.info("Nombre de lignes insérées/mises à jour reçu: %s", nb_inseres)
        return rapport

    @task(task_id="analyser_tendances")
    def analyser_tendances(rapport: str) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        latest = hook.get_first(
            """
            SELECT annee, mois, prix_m2_median_paris
            FROM stats_marche
            ORDER BY annee DESC, mois DESC
            LIMIT 1
            """
        )
        if not latest:
            msg = "Aucune donnée disponible pour analyser les tendances."
            logger.info(msg)
            return msg

        annee, mois, median_now = latest

        if int(mois) == 1:
            prev_annee = int(annee) - 1
            prev_mois = 12
        else:
            prev_annee = int(annee)
            prev_mois = int(mois) - 1

        previous = hook.get_first(
            """
            SELECT prix_m2_median_paris
            FROM stats_marche
            WHERE annee = %s AND mois = %s
            LIMIT 1
            """,
            parameters=(prev_annee, prev_mois),
        )

        if not previous or previous[0] is None:
            msg = (
                "Aucune tendance calculable : "
                "pas de données pour comparer avec le mois précédent."
            )
            logger.info(msg)
            return msg

        median_prev = float(previous[0])
        variation = round(((float(median_now) - median_prev) / median_prev) * 100, 2) if median_prev != 0 else None

        hook.run(
            """
            UPDATE stats_marche
            SET variation_median_pct = %s
            WHERE annee = %s AND mois = %s
            """,
            parameters=(variation, annee, mois),
        )

        message = (
            f"Tendance mensuelle Paris: {mois:02d}/{annee} vs {prev_mois:02d}/{prev_annee} "
            f"=> variation du prix médian = {variation:.2f}%"
        )
        logger.info(message)
        return message

    t_verif = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs_raw = stocker_hdfs_raw(t_download)
    t_partitions = partitionner_et_stocker_hdfs(t_download)
    t_traiter = traiter_donnees(t_partitions)
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_pg)
    t_tendance = analyser_tendances(t_rapport)

    chain(t_verif, t_download, t_hdfs_raw, t_partitions, t_traiter, t_pg, t_rapport, t_tendance)


pipeline_dvf()