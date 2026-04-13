
"""
Calcul des indicateurs épidémiques IAS — ARS Occitanie
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import numpy as np
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculer_zscore(valeur_actuelle: float, historique: list[Optional[float]]) -> Optional[float]:
    """
    Calcule le z-score à partir des valeurs historiques.
    Au moins 3 saisons valides sont requises.
    """
    valeurs_valides = [v for v in historique if v is not None]
    if len(valeurs_valides) < 3:
        logger.warning("Historique insuffisant pour le z-score : %s saisons", len(valeurs_valides))
        return None

    moyenne = float(np.mean(valeurs_valides))
    ecart_type = float(np.std(valeurs_valides, ddof=1))

    if ecart_type == 0:
        return 0.0

    return round((valeur_actuelle - moyenne) / ecart_type, 3)


def classifier_statut_ias(
    valeur_ias: float,
    seuil_min: Optional[float],
    seuil_max: Optional[float],
) -> str:
    """Classifie selon les seuils MIN/MAX IAS."""
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_zscore(
    z_score: Optional[float],
    seuil_alerte_z: float = 1.5,
    seuil_urgence_z: float = 3.0,
) -> str:
    """Classifie selon le z-score."""
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_final(statut_ias: str, statut_zscore: str) -> str:
    """Retient le niveau le plus sévère."""
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"


def calculer_r0_simplifie(series_hebdomadaire: list[float], duree_infectieuse: int = 5) -> Optional[float]:
    """
    Estimation du R0 à partir des 4 dernières valeurs IAS.
    """
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]
    if len(series_valides) < 2:
        return None

    croissances = [
        (series_valides[i] - series_valides[i - 1]) / series_valides[i - 1]
        for i in range(1, len(series_valides))
        if series_valides[i - 1] > 0
    ]

    if not croissances:
        return None

    r0 = 1 + (float(np.mean(croissances)) * (duree_infectieuse / 7))
    return round(max(0.0, r0), 3)


def calculer_taux_incidence_100k(valeur_ias: float, population_regionale: int) -> float:
    """
    Approximation du taux pour 100 000 habitants.
    Ici on rapporte la valeur IAS régionale à la population d'Occitanie.
    """
    if population_regionale <= 0:
        return 0.0
    return round((valeur_ias / population_regionale) * 100000, 3)


def get_population_occitanie(pg_conn_str: str) -> int:
    """Somme des populations des 13 départements."""
    with psycopg2.connect(pg_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(population), 0) FROM departements WHERE code_region = '76'")
            row = cur.fetchone()
            return int(row[0] or 0)


def get_historique_hebdomadaire(pg_conn_str: str, syndrome: str, limite: int = 4) -> list[float]:
    """Récupère les dernières valeurs IAS en base pour calculer le R0."""
    with psycopg2.connect(pg_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT valeur_ias
                FROM donnees_hebdomadaires
                WHERE syndrome = %s
                ORDER BY annee DESC, numero_semaine DESC
                LIMIT %s
                """,
                (syndrome, limite),
            )
            rows = cur.fetchall()

    valeurs = [float(row[0]) for row in rows if row[0] is not None]
    valeurs.reverse()
    return valeurs


def get_duree_infectieuse(pg_conn_str: str, syndrome: str) -> int:
    """Récupère la durée infectieuse depuis la table des syndromes."""
    with psycopg2.connect(pg_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT duree_infectieuse_jours FROM syndromes WHERE code = %s",
                (syndrome,),
            )
            row = cur.fetchone()

    return int(row[0]) if row and row[0] is not None else 5


def calculer_indicateurs_depuis_fichier(
    input_json_path: str,
    output_json_path: str,
    pg_conn_str: str,
    seuil_alerte_zscore: float = 1.5,
    seuil_urgence_zscore: float = 3.0,
) -> str:
    """Lit le JSON brut et produit le JSON d'indicateurs."""
    if not os.path.exists(input_json_path):
        raise FileNotFoundError(f"Fichier brut introuvable : {input_json_path}")

    with open(input_json_path, "r", encoding="utf-8") as file:
        payload = json.load(file)

    semaine = payload["semaine"]
    syndromes = payload["syndromes"]
    population_occitanie = get_population_occitanie(pg_conn_str)

    resultats: list[dict] = []

    for syndrome, data in syndromes.items():
        valeur_ias = data.get("valeur_ias")
        if valeur_ias is None:
            logger.warning("Aucune valeur IAS trouvée pour %s semaine %s", syndrome, semaine)
            continue

        historique_dict = data.get("historique", {})
        historique = [
            historique_dict.get("Sais_2023_2024"),
            historique_dict.get("Sais_2022_2023"),
            historique_dict.get("Sais_2021_2022"),
            historique_dict.get("Sais_2020_2021"),
            historique_dict.get("Sais_2019_2020"),
        ]
        nb_saisons = len([v for v in historique if v is not None])

        z_score = calculer_zscore(valeur_ias, historique)
        statut_ias = classifier_statut_ias(valeur_ias, data.get("seuil_min"), data.get("seuil_max"))
        statut_zscore = classifier_statut_zscore(
            z_score,
            seuil_alerte_z=seuil_alerte_zscore,
            seuil_urgence_z=seuil_urgence_zscore,
        )
        statut_final = classifier_statut_final(statut_ias, statut_zscore)

        serie_r0 = get_historique_hebdomadaire(pg_conn_str, syndrome, limite=4)
        serie_r0.append(valeur_ias)
        duree_infectieuse = get_duree_infectieuse(pg_conn_str, syndrome)
        r0_estime = calculer_r0_simplifie(serie_r0, duree_infectieuse)

        taux_incidence = calculer_taux_incidence_100k(valeur_ias, population_occitanie)

        commentaire = (
            f"Classification basée sur IAS={statut_ias} et z-score={statut_zscore}. "
            f"Valeur hebdomadaire Occitanie : {valeur_ias}."
        )

        resultats.append(
            {
                "semaine": semaine,
                "syndrome": syndrome,
                "valeur_ias": valeur_ias,
                "taux_incidence": taux_incidence,
                "z_score": z_score,
                "r0_estime": r0_estime,
                "nb_saisons_reference": nb_saisons,
                "statut": statut_final,
                "statut_ias": statut_ias,
                "statut_zscore": statut_zscore,
                "commentaire": commentaire,
                "seuil_min_saison": data.get("seuil_min"),
                "seuil_max_saison": data.get("seuil_max"),
                "nb_jours_donnees": data.get("nb_jours", 0),
            }
        )

    os.makedirs(os.path.dirname(output_json_path), exist_ok=True)

    with open(output_json_path, "w", encoding="utf-8") as file:
        json.dump(resultats, file, ensure_ascii=False, indent=2)

    logger.info("Indicateurs calculés pour %s et enregistrés dans %s", semaine, output_json_path)
    return output_json_path


if __name__ == "__main__":
    input_json = os.environ.get("INPUT_JSON", "/data/ars/raw/ias_latest.json")
    output_json = os.environ.get("OUTPUT_JSON", "/data/ars/indicateurs/indicateurs_latest.json")
    pg_conn = os.environ.get("POSTGRES_ARS_DSN", "dbname=ars_epidemio user=postgres password=postgres host=postgres-ars port=5432")

    chemin = calculer_indicateurs_depuis_fichier(
        input_json_path=input_json,
        output_json_path=output_json,
        pg_conn_str=pg_conn,
    )
    print(f"INDICATEURS_OK:{chemin}")