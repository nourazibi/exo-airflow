from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

sys.path.insert(0, "/opt/airflow/scripts")
from collecte_ias import collecter_ias_semaine  # noqa: E402
from calcul_indicateurs import calculer_indicateurs_depuis_fichier  # noqa: E402


default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def get_semaine_from_context(context: dict[str, Any]) -> str:
    """Construit la semaine ISO au format YYYY-SXX."""
    logical_date = context["logical_date"]
    iso_year, iso_week, _ = logical_date.isocalendar()
    return f"{iso_year}-S{iso_week:02d}"


def verifier_connexions_et_variables() -> None:
    """Vérifie les connexions et variables Airflow attendues."""
    conn_pg = BaseHook.get_connection("postgres_ars")
    depts = Variable.get(
        "departements_occitanie",
        default_var='["09","11","12","30","31","32","34","46","48","65","66","81","82"]',
        deserialize_json=True,
    )

    logger.info(
        "Connexion PostgreSQL OK : %s:%s/%s",
        conn_pg.host,
        conn_pg.port,
        conn_pg.schema,
    )
    logger.info("Nombre de départements configurés : %s", len(depts))


def collecter_donnees_ias_task(**context: Any) -> str:
    """Télécharge les CSV IAS et retourne le chemin du JSON brut."""
    semaine = get_semaine_from_context(context)
    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir = f"{archive_path}/raw"
    return collecter_ias_semaine(semaine=semaine, output_dir=output_dir)


def archiver_local(**context: Any) -> str:
    """Archive le fichier brut dans /data/ars/raw/YYYY/SXX/."""
    semaine = get_semaine_from_context(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin_source = context["ti"].xcom_pull(task_ids="collecte.collecter_donnees_sursaud")
    if not chemin_source or not os.path.exists(chemin_source):
        raise FileNotFoundError(f"Fichier source introuvable : {chemin_source}")

    archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
    os.makedirs(archive_dir, exist_ok=True)

    chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"
    shutil.copy2(chemin_source, chemin_dest)

    print(f"ARCHIVE_OK:{chemin_dest}")
    return chemin_dest


def verifier_archive(**context: Any) -> bool:
    """Vérifie l'existence et la non-vacuité du fichier archivé."""
    semaine = get_semaine_from_context(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante : {chemin}")

    taille = os.path.getsize(chemin)
    if taille == 0:
        raise ValueError(f"Archive vide : {chemin}")

    print(f"ARCHIVE_VALIDE:{chemin} ({taille} octets)")
    return True


def calculer_indicateurs_epidemiques(**context: Any) -> str:
    """Calcule les indicateurs et produit un JSON de sortie."""
    semaine = get_semaine_from_context(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    input_json = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    output_json = f"/data/ars/indicateurs/indicateurs_{semaine}.json"

    conn = BaseHook.get_connection("postgres_ars")
    pg_dsn = (
        f"dbname={conn.schema} "
        f"user={conn.login} "
        f"password={conn.password} "
        f"host={conn.host} "
        f"port={conn.port}"
    )

    seuil_alerte_zscore = float(Variable.get("seuil_alerte_zscore", default_var="1.5"))
    seuil_urgence_zscore = float(Variable.get("seuil_urgence_zscore", default_var="3.0"))

    return calculer_indicateurs_depuis_fichier(
        input_json_path=input_json,
        output_json_path=output_json,
        pg_conn_str=pg_dsn,
        seuil_alerte_zscore=seuil_alerte_zscore,
        seuil_urgence_zscore=seuil_urgence_zscore,
    )


def inserer_donnees_postgres(**context: Any) -> None:
    """Insère les données hebdomadaires et indicateurs avec ON CONFLICT DO UPDATE."""
    semaine = get_semaine_from_context(context)
    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]

    chemin_brut = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    chemin_indicateurs = f"/data/ars/indicateurs/indicateurs_{semaine}.json"

    if not os.path.exists(chemin_brut):
        raise FileNotFoundError(f"JSON brut introuvable : {chemin_brut}")
    if not os.path.exists(chemin_indicateurs):
        raise FileNotFoundError(f"JSON indicateurs introuvable : {chemin_indicateurs}")

    with open(chemin_brut, "r", encoding="utf-8") as file:
        donnees_brutes = json.load(file)

    with open(chemin_indicateurs, "r", encoding="utf-8") as file:
        indicateurs = json.load(file)

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    sql_donnees = """
    INSERT INTO donnees_hebdomadaires
    (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
    VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(seuil_min_saison)s, %(seuil_max_saison)s, %(nb_jours_donnees)s)
    ON CONFLICT (semaine, syndrome)
    DO UPDATE SET
        valeur_ias = EXCLUDED.valeur_ias,
        seuil_min_saison = EXCLUDED.seuil_min_saison,
        seuil_max_saison = EXCLUDED.seuil_max_saison,
        nb_jours_donnees = EXCLUDED.nb_jours_donnees,
        updated_at = CURRENT_TIMESTAMP;
    """

    sql_indicateurs = """
    INSERT INTO indicateurs_epidemiques
    (semaine, syndrome, valeur_ias, taux_incidence, z_score, r0_estime, nb_saisons_reference, statut, statut_ias, statut_zscore, commentaire)
    VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(taux_incidence)s, %(z_score)s, %(r0_estime)s, %(nb_saisons_reference)s, %(statut)s, %(statut_ias)s, %(statut_zscore)s, %(commentaire)s)
    ON CONFLICT (semaine, syndrome)
    DO UPDATE SET
        valeur_ias = EXCLUDED.valeur_ias,
        taux_incidence = EXCLUDED.taux_incidence,
        z_score = EXCLUDED.z_score,
        r0_estime = EXCLUDED.r0_estime,
        nb_saisons_reference = EXCLUDED.nb_saisons_reference,
        statut = EXCLUDED.statut,
        statut_ias = EXCLUDED.statut_ias,
        statut_zscore = EXCLUDED.statut_zscore,
        commentaire = EXCLUDED.commentaire,
        updated_at = CURRENT_TIMESTAMP;
    """

    nb_donnees_inserees = 0
    nb_indicateurs_inseres = 0

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for syndrome, data in donnees_brutes.get("syndromes", {}).items():
                valeur_ias = data.get("valeur_ias")

                if valeur_ias is None:
                    logger.warning(
                        "Aucune donnée exploitable pour %s sur la semaine %s, insertion ignorée.",
                        syndrome,
                        semaine,
                    )
                    continue

                cur.execute(
                    sql_donnees,
                    {
                        "semaine": semaine,
                        "syndrome": syndrome,
                        "valeur_ias": valeur_ias,
                        "seuil_min_saison": data.get("seuil_min"),
                        "seuil_max_saison": data.get("seuil_max"),
                        "nb_jours_donnees": data.get("nb_jours", 0),
                    },
                )
                nb_donnees_inserees += 1

            for indicateur in indicateurs:
                if indicateur.get("valeur_ias") is None:
                    logger.warning(
                        "Indicateur ignoré car valeur_ias absente pour %s semaine %s",
                        indicateur.get("syndrome"),
                        indicateur.get("semaine"),
                    )
                    continue

                cur.execute(sql_indicateurs, indicateur)
                nb_indicateurs_inseres += 1

        conn.commit()

    logger.info(
        "%s données hebdomadaires et %s indicateurs insérés/mis à jour pour la semaine %s",
        nb_donnees_inserees,
        nb_indicateurs_inseres,
        semaine,
    )


def evaluer_situation_epidemique(**context: Any) -> str:
    """Détermine la branche à exécuter selon le statut le plus sévère."""
    semaine = get_semaine_from_context(context)
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT statut, COUNT(*) AS nb_syndromes
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
                """,
                (semaine,),
            )
            resultats = {row[0]: row[1] for row in cur.fetchall()}

    nb_urgence = resultats.get("URGENCE", 0)
    nb_alerte = resultats.get("ALERTE", 0)

    context["ti"].xcom_push(key="nb_urgence", value=nb_urgence)
    context["ti"].xcom_push(key="nb_alerte", value=nb_alerte)

    logger.info("Semaine %s : %s URGENCE, %s ALERTE", semaine, nb_urgence, nb_alerte)

    if nb_urgence > 0:
        return "declencher_alerte_ars"
    if nb_alerte > 0:
        return "envoyer_bulletin_surveillance"
    return "confirmer_situation_normale"


def declencher_alerte_ars(**context: Any) -> None:
    """Log d'une alerte urgente."""
    nb_urgence = context["ti"].xcom_pull(task_ids="evaluer_situation_epidemique", key="nb_urgence")
    logger.critical("ALERTE ARS DÉCLENCHÉE — %s syndrome(s) en URGENCE", nb_urgence)


def envoyer_bulletin_surveillance(**context: Any) -> None:
    """Log d'un bulletin de surveillance."""
    nb_alerte = context["ti"].xcom_pull(task_ids="evaluer_situation_epidemique", key="nb_alerte")
    logger.warning("Bulletin de surveillance envoyé — %s syndrome(s) en ALERTE", nb_alerte)


def confirmer_situation_normale(**context: Any) -> None:
    """Log d'une situation normale."""
    logger.info("Situation épidémiologique normale en Occitanie — aucune action requise")


def generer_rapport_hebdomadaire(**context: Any) -> None:
    """Génère le rapport JSON et l'enregistre aussi en base."""
    semaine = get_semaine_from_context(context)
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ie.syndrome,
                    s.libelle,
                    ie.valeur_ias,
                    ie.taux_incidence,
                    ie.z_score,
                    ie.r0_estime,
                    ie.statut
                FROM indicateurs_epidemiques ie
                JOIN syndromes s ON ie.syndrome = s.code
                WHERE ie.semaine = %s
                ORDER BY ie.statut DESC, ie.taux_incidence DESC NULLS LAST
                """,
                (semaine,),
            )
            indicateurs = cur.fetchall()

    statuts = [row[6] for row in indicateurs]
    if "URGENCE" in statuts:
        situation_globale = "URGENCE"
    elif "ALERTE" in statuts:
        situation_globale = "ALERTE"
    else:
        situation_globale = "NORMAL"

    recommandations_par_niveau = {
        "URGENCE": [
            "Activation du plan de réponse épidémique régional",
            "Renforcement des équipes de surveillance",
            "Communication renforcée auprès des partenaires de santé",
            "Notification immédiate aux autorités concernées",
        ],
        "ALERTE": [
            "Surveillance renforcée des indicateurs pour les 48h suivantes",
            "Envoi d'un bulletin de surveillance",
            "Vérification des capacités de réponse des services de santé",
        ],
        "NORMAL": [
            "Maintien de la surveillance standard",
            "Prochain point épidémiologique dans 7 jours",
        ],
    }

    rapport = {
        "semaine": semaine,
        "region": "Occitanie",
        "code_region": "76",
        "date_generation": datetime.utcnow().isoformat(),
        "situation_globale": situation_globale,
        "nb_departements_surveilles": 13,
        "nb_depts_alerte": sum(1 for row in indicateurs if row[6] == "ALERTE"),
        "nb_depts_urgence": sum(1 for row in indicateurs if row[6] == "URGENCE"),
        "indicateurs": [
            {
                "syndrome": row[0],
                "libelle": row[1],
                "valeur_ias": row[2],
                "taux_incidence_100k": row[3],
                "z_score": row[4],
                "r0_estime": row[5],
                "statut": row[6],
            }
            for row in indicateurs
        ],
        "recommandations": recommandations_par_niveau[situation_globale],
        "genere_par": "ars_epidemio_dag v1.0",
        "pipeline_version": "2.8",
    }

    annee = semaine.split("-")[0]
    num_sem = semaine.split("-")[1]
    local_path = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as file:
        json.dump(rapport, file, ensure_ascii=False, indent=2)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rapports_ars
                (semaine, situation_globale, nb_depts_alerte, nb_depts_urgence, rapport_json, chemin_local)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (semaine)
                DO UPDATE SET
                    situation_globale = EXCLUDED.situation_globale,
                    nb_depts_alerte = EXCLUDED.nb_depts_alerte,
                    nb_depts_urgence = EXCLUDED.nb_depts_urgence,
                    rapport_json = EXCLUDED.rapport_json,
                    chemin_local = EXCLUDED.chemin_local,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    semaine,
                    situation_globale,
                    rapport["nb_depts_alerte"],
                    rapport["nb_depts_urgence"],
                    json.dumps(rapport, ensure_ascii=False),
                    local_path,
                ),
            )
        conn.commit()

    logger.info("Rapport %s généré — statut global : %s", semaine, situation_globale)


with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline de surveillance épidémiologique ARS Occitanie",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:
    verifier_config = PythonOperator(
        task_id="verifier_connexions_et_variables",
        python_callable=verifier_connexions_et_variables,
    )

    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    with TaskGroup("collecte") as collecte:
        collecter_sursaud = PythonOperator(
            task_id="collecter_donnees_sursaud",
            python_callable=collecter_donnees_ias_task,
        )

    with TaskGroup("persistance_brute") as persistance_brute:
        archiver = PythonOperator(
            task_id="archiver_local",
            python_callable=archiver_local,
        )

        verifier = PythonOperator(
            task_id="verifier_archive",
            python_callable=verifier_archive,
        )

        archiver >> verifier

    with TaskGroup("traitement") as traitement:
        calculer = PythonOperator(
            task_id="calculer_indicateurs_epidemiques",
            python_callable=calculer_indicateurs_epidemiques,
        )

    with TaskGroup("persistance_operationnelle") as persistance_operationnelle:
        inserer_postgres = PythonOperator(
            task_id="inserer_donnees_postgres",
            python_callable=inserer_donnees_postgres,
        )

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
    )

    alerte_ars = PythonOperator(
        task_id="declencher_alerte_ars",
        python_callable=declencher_alerte_ars,
    )

    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance,
    )

    normale = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale,
    )

    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    verifier_config >> init_base_donnees >> collecte >> persistance_brute >> traitement >> persistance_operationnelle >> evaluer
    evaluer >> [alerte_ars, bulletin, normale] >> generer_rapport