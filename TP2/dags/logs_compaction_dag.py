from datetime import datetime, timedelta, date
import tempfile
import os
import requests

from airflow.decorators import dag, task


WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "root"


def webhdfs_liststatus(hdfs_path: str) -> list:
    url = f"{WEBHDFS_BASE}{hdfs_path}"
    resp = requests.get(
        url,
        params={"op": "LISTSTATUS", "user.name": HDFS_USER},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["FileStatuses"]["FileStatus"]


def webhdfs_open_text(hdfs_path: str) -> str:
    url = f"{WEBHDFS_BASE}{hdfs_path}"
    resp = requests.get(
        url,
        params={"op": "OPEN", "user.name": HDFS_USER},
        timeout=30,
        allow_redirects=True,
    )
    resp.raise_for_status()
    return resp.text


def webhdfs_create_file(local_path: str, hdfs_path: str) -> None:
    create_url = f"{WEBHDFS_BASE}{hdfs_path}"
    r1 = requests.put(
        create_url,
        params={"op": "CREATE", "overwrite": "true", "user.name": HDFS_USER},
        allow_redirects=False,
        timeout=20,
    )
    if r1.status_code not in (307, 201):
        raise RuntimeError(f"Erreur CREATE WebHDFS: {r1.status_code} - {r1.text}")

    upload_url = r1.headers.get("Location")
    if not upload_url:
        raise RuntimeError("Aucune redirection fournie par WebHDFS.")

    with open(local_path, "rb") as f:
        r2 = requests.put(upload_url, data=f, timeout=60)

    if r2.status_code not in (200, 201):
        raise RuntimeError(f"Erreur upload WebHDFS: {r2.status_code} - {r2.text}")


def webhdfs_delete(hdfs_path: str) -> None:
    url = f"{WEBHDFS_BASE}{hdfs_path}"
    resp = requests.delete(
        url,
        params={"op": "DELETE", "recursive": "false", "user.name": HDFS_USER},
        timeout=20,
    )
    resp.raise_for_status()
    ok = resp.json().get("boolean", False)
    if not ok:
        raise RuntimeError(f"Suppression refusée pour {hdfs_path}")


def compter_lignes_texte(texte: str) -> int:
    if not texte.strip():
        return 0
    return len(texte.strip().splitlines())


@dag(
    dag_id="logs_compaction_dag",
    schedule="0 3 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["hdfs", "compaction", "maintenance"],
)
def logs_compaction_dag():

    @task()
    def lister_fichiers_semaine() -> list[str]:
        """
        Liste les fichiers journaliers de la semaine précédente dans HDFS/raw.
        """
        raw_path = "/data/ecommerce/logs/raw"
        fichiers = webhdfs_liststatus(raw_path)

        today = date.today()
        start_period = today - timedelta(days=7)
        end_period = today - timedelta(days=1)

        selection = []

        for f in fichiers:
            name = f["pathSuffix"]  # ex: access_2026-04-09.log
            if not name.startswith("access_") or not name.endswith(".log"):
                continue

            date_str = name.replace("access_", "").replace(".log", "")
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            if start_period <= file_date <= end_period:
                selection.append(f"{raw_path}/{name}")

        if not selection:
            raise ValueError("Aucun fichier trouvé pour la semaine précédente dans raw.")

        return sorted(selection)

    @task()
    def fusionner_fichiers(fichiers: list[str]) -> str:
        """
        Lit les fichiers HDFS, les concatène et écrit un fichier weekly.
        """
        today = date.today()
        semaine_precedente = today - timedelta(days=7)
        iso_year, iso_week, _ = semaine_precedente.isocalendar()

        weekly_hdfs_path = f"/data/ecommerce/logs/weekly/{iso_year}-W{iso_week:02d}.log"

        os.makedirs("/tmp", exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8", suffix=".log") as tmp:
            tmp_path = tmp.name
            for chemin in fichiers:
                contenu = webhdfs_open_text(chemin)
                tmp.write(contenu)
                if contenu and not contenu.endswith("\n"):
                    tmp.write("\n")

        webhdfs_create_file(tmp_path, weekly_hdfs_path)
        os.remove(tmp_path)

        return weekly_hdfs_path

    @task()
    def valider_compaction(chemin_weekly: str, fichiers_source: list[str]) -> None:
        """
        Vérifie que le fichier weekly contient bien la somme des lignes source.
        """
        total_source = 0
        for chemin in fichiers_source:
            contenu = webhdfs_open_text(chemin)
            total_source += compter_lignes_texte(contenu)

        contenu_weekly = webhdfs_open_text(chemin_weekly)
        total_weekly = compter_lignes_texte(contenu_weekly)

        if total_weekly < total_source:
            raise ValueError(
                f"Validation échouée : weekly={total_weekly} lignes, sources={total_source} lignes."
            )

    @task()
    def supprimer_fichiers_journaliers(fichiers: list[str]) -> None:
        """
        Supprime les fichiers journaliers après validation.
        """
        for chemin in fichiers:
            webhdfs_delete(chemin)

    fichiers = lister_fichiers_semaine()
    weekly = fusionner_fichiers(fichiers)
    validation = valider_compaction(weekly, fichiers)
    validation >> supprimer_fichiers_journaliers(fichiers)


logs_compaction_dag()