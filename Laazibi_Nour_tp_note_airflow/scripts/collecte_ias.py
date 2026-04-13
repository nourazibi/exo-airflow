
"""
Collecte des données IAS® (Indicateur Avancé Sanitaire)
pour la région Occitanie depuis data.gouv.fr.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import date, datetime
from typing import Optional

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASETS_IAS: dict[str, str] = {
    "GRIPPE": "https://www.data.gouv.fr/api/1/datasets/r/35f46fbb-7a97-46b3-a93c-35a471033447",
    "GEA": "https://www.data.gouv.fr/api/1/datasets/r/6c415be9-4ebf-4af5-b0dc-9867bb1ec0e3",
}

# Les datasets IAS utilisent les anciens codes régions pré-2016.
# Occitanie = moyenne de Languedoc-Roussillon (Loc_Reg91)
# et Midi-Pyrénées (Loc_Reg73).
COLS_OCCITANIE = ["Loc_Reg91", "Loc_Reg73"]

SAISONS_COLS = [
    "Sais_2023_2024",
    "Sais_2022_2023",
    "Sais_2021_2022",
    "Sais_2020_2021",
    "Sais_2019_2020",
]


def get_semaine_iso(reference_date: Optional[date] = None) -> str:
    """Retourne la semaine ISO au format YYYY-SXX."""
    if reference_date is None:
        reference_date = date.today()
    iso_year, iso_week, _ = reference_date.isocalendar()
    return f"{iso_year}-S{iso_week:02d}"


def telecharger_csv_ias(url: str) -> list[dict]:
    """
    Télécharge et parse un CSV IAS depuis data.gouv.fr.
    Séparateur ';', décimaux ','.
    """
    logger.info("Téléchargement du dataset IAS : %s", url)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    content = response.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")

    rows: list[dict] = []
    for row in reader:
        cleaned_row: dict[str, Optional[str]] = {}
        for key, value in row.items():
            if value in ("NA", "", None):
                cleaned_row[key] = None
            else:
                cleaned_row[key] = value.replace(",", ".")
        rows.append(cleaned_row)

    logger.info("%s lignes récupérées", len(rows))
    return rows


def filtrer_semaine(rows: list[dict], semaine: str) -> list[dict]:
    """Filtre les lignes correspondant à une semaine ISO donnée."""
    annee_cible = int(semaine[:4])
    num_sem_cible = int(semaine.split("-S")[1])

    filtered: list[dict] = []

    for row in rows:
        periode = row.get("PERIODE")
        if not periode:
            continue

        try:
            d = datetime.strptime(periode, "%d-%m-%Y").date()
            iso_year, iso_week, _ = d.isocalendar()
            if iso_year == annee_cible and iso_week == num_sem_cible:
                filtered.append(row)
        except ValueError:
            logger.warning("Date invalide ignorée : %s", periode)

    logger.info("%s jours trouvés pour la semaine %s", len(filtered), semaine)
    return filtered


def safe_float(value: Optional[str]) -> Optional[float]:
    """Convertit une chaîne en float si possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_mean(values: list[float]) -> Optional[float]:
    """Retourne la moyenne arrondie d'une liste non vide."""
    if not values:
        return None
    return round(sum(values) / len(values), 3)


def agreger_semaine(rows: list[dict], syndrome: str, semaine: str) -> dict:
    """
    Agrège les valeurs de la semaine pour un syndrome.
    Calcule :
    - valeur_ias = moyenne de Loc_Reg91 et Loc_Reg73
    - seuil_min / seuil_max
    - historique = moyenne hebdo des colonnes historiques
    """
    valeurs_ias: list[float] = []
    min_saison_vals: list[float] = []
    max_saison_vals: list[float] = []
    historique: dict[str, list[float]] = {col: [] for col in SAISONS_COLS}

    for row in rows:
        vals_occitanie: list[float] = []
        for col in COLS_OCCITANIE:
            v = safe_float(row.get(col))
            if v is not None:
                vals_occitanie.append(v)

        val_ias = safe_mean(vals_occitanie)
        if val_ias is not None:
            valeurs_ias.append(val_ias)

        min_val = safe_float(row.get("MIN_Saison"))
        if min_val is not None:
            min_saison_vals.append(min_val)

        max_val = safe_float(row.get("MAX_Saison"))
        if max_val is not None:
            max_saison_vals.append(max_val)

        for col in SAISONS_COLS:
            v = safe_float(row.get(col))
            if v is not None:
                historique[col].append(v)

    return {
        "semaine": semaine,
        "syndrome": syndrome,
        "valeur_ias": safe_mean(valeurs_ias),
        "seuil_min": safe_mean(min_saison_vals),
        "seuil_max": safe_mean(max_saison_vals),
        "nb_jours": len(valeurs_ias),
        "historique": {col: safe_mean(vals) for col, vals in historique.items()},
    }


def sauvegarder_donnees(donnees: dict, semaine: str, output_dir: str) -> str:
    """Sauvegarde les données agrégées dans un JSON brut."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"ias_{semaine}.json")

    payload = {
        "semaine": semaine,
        "collecte_le": datetime.utcnow().isoformat(),
        "source": "IAS_OpenHealth_data.gouv.fr",
        "syndromes": donnees,
    }

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    logger.info("Données sauvegardées dans %s", output_path)
    return output_path


def collecter_ias_semaine(semaine: str, output_dir: str) -> str:
    """Collecte et agrège les données IAS pour tous les syndromes disponibles."""
    resultats: dict[str, dict] = {}

    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

    return sauvegarder_donnees(resultats, semaine, output_dir)


if __name__ == "__main__":
    semaine_cible = os.environ.get("SEMAINE_CIBLE", get_semaine_iso())
    output_directory = os.environ.get("OUTPUT_DIR", "/data/ars/raw")
    chemin = collecter_ias_semaine(semaine_cible, output_directory)
    print(f"COLLECTE_OK:{chemin}") 