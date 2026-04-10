from datetime import datetime, timedelta
import logging
import os
import subprocess
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from hdfs_sensor import HdfsFileSensor


SEUIL_ERREUR_PCT = 5.0
WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "root"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def webhdfs_create_file(local_path: str, hdfs_path: str) -> None:
    create_url = f"{WEBHDFS_BASE}{hdfs_path}"
    params = {"op": "CREATE", "overwrite": "true", "user.name": HDFS_USER}

    r1 = requests.put(create_url, params=params, allow_redirects=False, timeout=30)
    if r1.status_code not in (307, 201):
        raise RuntimeError(f"Échec init CREATE WebHDFS: {r1.status_code} - {r1.text}")

    upload_url = r1.headers.get("Location")
    if not upload_url:
        raise RuntimeError("Aucune URL de redirection fournie par WebHDFS pour l'upload.")

    with open(local_path, "rb") as f:
        r2 = requests.put(upload_url, data=f, timeout=120)

    if r2.status_code not in (200, 201):
        raise RuntimeError(f"Échec upload WebHDFS: {r2.status_code} - {r2.text}")


def webhdfs_rename(src_path: str, dst_path: str) -> None:
    rename_url = f"{WEBHDFS_BASE}{src_path}"
    params = {
        "op": "RENAME",
        "destination": dst_path,
        "user.name": HDFS_USER,
    }
    r = requests.put(rename_url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Échec rename WebHDFS: {r.status_code} - {r.text}")

    data = r.json()
    if not data.get("boolean", False):
        raise RuntimeError("Le déplacement HDFS a retourné false.")


def webhdfs_exists(hdfs_path: str) -> bool:
    url = f"{WEBHDFS_BASE}{hdfs_path}"
    params = {"op": "GETFILESTATUS", "user.name": HDFS_USER}
    r = requests.get(url, params=params, timeout=15)
    return r.status_code == 200


def generer_logs_journaliers(**context):
    execution_date = context["ds"]
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"

    logging.info("Génération des logs pour la date %s", execution_date)

    subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        check=True,
    )

    if not os.path.exists(fichier_sortie):
        raise FileNotFoundError(f"Le fichier {fichier_sortie} n'a pas été créé.")

    taille = os.path.getsize(fichier_sortie)
    logging.info("Fichier généré : %s (%s octets)", fichier_sortie, taille)

    return fichier_sortie


def uploader_vers_hdfs(**context):
    execution_date = context["ds"]
    fichier_local = f"/tmp/access_{execution_date}.log"
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    if not os.path.exists(fichier_local):
        raise FileNotFoundError(f"Fichier local introuvable : {fichier_local}")

    logging.info("Upload de %s vers HDFS:%s", fichier_local, chemin_hdfs)
    webhdfs_create_file(fichier_local, chemin_hdfs)
    logging.info("Upload terminé avec succès")

    return chemin_hdfs


def brancher_selon_taux_erreur(**context):
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

    if not os.path.exists(fichier_taux):
        raise FileNotFoundError(f"Fichier taux introuvable : {fichier_taux}")

    with open(fichier_taux, "r", encoding="utf-8") as f:
        contenu = f.read().strip()

    erreurs_str, total_str = contenu.split()
    erreurs = int(erreurs_str)
    total = int(total_str)

    taux_pct = (erreurs / total) * 100 if total > 0 else 0.0
    logging.info("Taux d'erreur HTTP = %.2f%% (%d/%d)", taux_pct, erreurs, total)

    if taux_pct > SEUIL_ERREUR_PCT:
        return "alerter_equipe_ops"
    return "archiver_rapport_ok"


def alerter_equipe_ops(**context):
    execution_date = context["ds"]
    logging.warning(
        "[ALERTE] Taux d'erreur HTTP anormal détecté pour les logs du %s. "
        "Vérifiez les serveurs web.",
        execution_date,
    )


def archiver_rapport_ok(**context):
    execution_date = context["ds"]
    logging.info(
        "[OK] Taux d'erreur dans les seuils normaux pour les logs du %s.",
        execution_date,
    )


def archiver_logs_hdfs(**context):
    execution_date = context["ds"]
    source = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
    destination = f"/data/ecommerce/logs/processed/access_{execution_date}.log"

    if webhdfs_exists(destination):
        logging.info("Le fichier est déjà présent dans processed : %s", destination)
        return

    if not webhdfs_exists(source):
        logging.info("Le fichier source n'existe plus dans raw : %s", source)
        return

    logging.info("Déplacement HDFS : %s -> %s", source, destination)
    webhdfs_rename(source, destination)
    logging.info("Fichier archivé dans la zone processed")


with DAG(
    dag_id="logs_ecommerce_dag",
    default_args=default_args,
    description="Pipeline d'ingestion et d'analyse de logs e-commerce vers HDFS",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["hdfs", "airflow", "ecommerce", "logs"],
) as dag:

    t_generer = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers,
    )

    t_upload = PythonOperator(
        task_id="uploader_vers_hdfs",
        python_callable=uploader_vers_hdfs,
    )

    t_sensor = HdfsFileSensor(
    task_id="hdfs_file_sensor",
    hdfs_path="/data/ecommerce/logs/raw/access_{{ ds }}.log",
    namenode_url="http://namenode:9870",
    poke_interval=30,
    timeout=300,
    mode="reschedule",
)

    t_analyser = BashOperator(
    task_id="analyser_logs_hdfs",
    bash_command=r"""
EXECUTION_DATE="{{ ds }}"
CHEMIN_RAW="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
CHEMIN_PROCESSED="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"
FICHIER_LOCAL="/tmp/logs_analyse_${EXECUTION_DATE}.txt"
FICHIER_TAUX="/tmp/taux_erreur_${EXECUTION_DATE}.txt"

HTTP_RAW=$(curl -s -o /dev/null -w "%{http_code}" \
"http://namenode:9870/webhdfs/v1${CHEMIN_RAW}?op=GETFILESTATUS&user.name=root")

if [ "$HTTP_RAW" -eq 200 ]; then
  CHEMIN_HDFS="${CHEMIN_RAW}"
else
  CHEMIN_HDFS="${CHEMIN_PROCESSED}"
fi

echo "[INFO] Lecture du fichier HDFS : ${CHEMIN_HDFS}"

curl -L -s "http://namenode:9870/webhdfs/v1${CHEMIN_HDFS}?op=OPEN&user.name=root" -o "${FICHIER_LOCAL}"

if [ ! -f "${FICHIER_LOCAL}" ]; then
  echo "[ERREUR] Le fichier local d'analyse n'existe pas"
  exit 1
fi

echo "[INFO] Nombre total de lignes dans le fichier :"
wc -l "${FICHIER_LOCAL}"

echo "=== STATUS CODES ==="
awk -F'"' '{print $2}' "${FICHIER_LOCAL}" | awk '{print $3}' | grep -E '^[0-9]{3}$' | sort | uniq -c | sort -rn

echo "=== TOP 5 URLS ==="
awk -F'"' '{print $2}' "${FICHIER_LOCAL}" | awk '{print $2}' | sort | uniq -c | sort -rn | head -5

TOTAL=$(awk -F'"' 'NF>=3 {count++} END {print count+0}' "${FICHIER_LOCAL}")
ERREURS=$(awk -F'"' 'NF>=3 {split($3,a," "); if (a[2] ~ /^(4|5)[0-9][0-9]$/) err++} END {print err+0}' "${FICHIER_LOCAL}")

echo "=== TAUX ERREUR ==="
echo "Total: ${TOTAL}, Erreurs: ${ERREURS}"

echo "${ERREURS} ${TOTAL}" > "${FICHIER_TAUX}"
echo "[OK] Analyse terminée"
""",
)

    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur,
    )

    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )

    t_archiver = PythonOperator(
        task_id="archiver_logs_hdfs",
        python_callable=archiver_logs_hdfs,
        trigger_rule="none_failed_min_one_success",
    )

    (
        t_generer
        >> t_upload
        >> t_sensor
        >> t_analyser
        >> t_branch
        >> [t_alerte, t_archive_ok]
        >> t_archiver
    )