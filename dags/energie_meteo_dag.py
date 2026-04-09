
# DAG : permet de créer le workflow Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import pendulum
import requests
import logging
import json


# CONFIGURATION GLOBALE


local_tz = pendulum.timezone("Europe/Paris")


REGIONS = {
    "Île-de-France": {"lat": 48.8566, "lon": 2.3522},
    "Occitanie": {"lat": 43.6047, "lon": 1.4442},
    "Nouvelle-Aquitaine": {"lat": 44.8378, "lon": -0.5792},
    "Auvergne-Rhône-Alpes": {"lat": 45.7640, "lon": 4.8357},
    "Hauts-de-France": {"lat": 50.6292, "lon": 3.0573},
}

# Paramètres par défaut des tâches du DAG
default_args = {
    "owner": "rte-data-team",           # propriétaire logique du DAG
    "depends_on_past": False,           # une exécution ne dépend pas de la précédente
    "email_on_failure": False,          # pas d'envoi d'email en cas d'erreur
    "retries": 2,                       # si échec, la tâche peut être relancée 2 fois
    "retry_delay": timedelta(minutes=5) # délai entre deux tentatives
}



# TÂCHE 1 : VERIFIER LES APIS


def verifier_apis(**context):


    # URLs minimales de test pour vérifier que les APIs répondent
    apis = {
        "Open-Meteo": (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=48.8566&longitude=2.3522"
            "&daily=sunshine_duration&timezone=Europe/Paris&forecast_days=1"
        ),
        "éCO2mix": (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
            "/eco2mix-regional-cons-def/records?limit=1&timezone=Europe%2FParis"
        ),
    }

    # On teste chaque API une par une
    for nom, url in apis.items():
        try:
            # Appel HTTP GET avec timeout pour éviter de bloquer indéfiniment
            response = requests.get(url, timeout=10)

            # Si le status code n’est pas 200, on considère que l’API n’est pas utilisable
            if response.status_code != 200:
                raise ValueError(f"API {nom} indisponible, code={response.status_code}")

            # Log d’information si tout va bien
            logging.info("API %s disponible", nom)

        except Exception as e:
            # En cas d’erreur, on stoppe le pipeline immédiatement
            raise ValueError(f"Erreur API {nom} : {e}")

    logging.info("Toutes les APIs sont disponibles. Pipeline autorisé à continuer.")


# TÂCHE 2 : COLLECTER LA MÉTÉO


def collecter_meteo_regions(**context):

    base_url = "https://api.open-meteo.com/v1/forecast"

    # Dictionnaire final des résultats
    resultats = {}

    # Boucle sur chaque région
    for region, coords in REGIONS.items():

        # Paramètres de la requête pour Open-Meteo
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }

        # Appel à l’API météo
        response = requests.get(base_url, params=params, timeout=15)

        # Déclenche une exception HTTP si réponse 4xx/5xx
        response.raise_for_status()

        # Conversion de la réponse en JSON Python
        data = response.json()

        # Le bloc "daily" contient les valeurs journalières
        daily = data.get("daily", {})

        # sunshine_duration est en secondes -> on convertit en heures
        sunshine_seconds = (daily.get("sunshine_duration") or [0])[0] or 0

        # vitesse max du vent à 10m
        vent_kmh = (daily.get("wind_speed_10m_max") or [0])[0] or 0

        # On stocke la région dans le dictionnaire final
        resultats[region] = {
            "ensoleillement_h": round(sunshine_seconds / 3600, 2),
            "vent_kmh": float(vent_kmh),
        }

        # Log utile pour suivre la collecte
        logging.info("%s -> %s", region, resultats[region])

    # Ce return sera poussé automatiquement dans XCom
    return resultats



# TÂCHE 3 : COLLECTER LA PRODUCTION ÉLECTRIQUE


def collecter_production_electrique(**context):

    Retour :
    {
        "Île-de-France": {"solaire_mw": 320.5, "eolien_mw": 82.0},
        ...
    }

    base_url = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
        "/eco2mix-regional-cons-def/records"
    )

    params = {
        "limit": 100,
        "timezone": "Europe/Paris",
    }

    # Appel à l’API éCO2mix
    response = requests.get(base_url, params=params, timeout=15)
    response.raise_for_status()

    # Liste des enregistrements retournés
    results = response.json().get("results", [])

    # Dictionnaire temporaire pour accumuler les valeurs
    accumulation = {
        region: {"solaire": [], "eolien": []}
        for region in REGIONS
    }

    # On parcourt tous les enregistrements de l’API
    for enregistrement in results:
        region = enregistrement.get("libelle_region")

        # On ne garde que les régions de notre liste
        if region in REGIONS:
            # Certaines valeurs peuvent être null -> on remplace par 0.0
            solaire = enregistrement.get("solaire") or 0.0
            eolien = enregistrement.get("eolien") or 0.0

            accumulation[region]["solaire"].append(float(solaire))
            accumulation[region]["eolien"].append(float(eolien))

    # Dictionnaire final
    production = {}

    # Calcul des moyennes par région
    for region, valeurs in accumulation.items():
        solaire_vals = valeurs["solaire"]
        eolien_vals = valeurs["eolien"]

        production[region] = {
            "solaire_mw": round(sum(solaire_vals) / len(solaire_vals), 2) if solaire_vals else 0.0,
            "eolien_mw": round(sum(eolien_vals) / len(eolien_vals), 2) if eolien_vals else 0.0,
        }

        logging.info("Production %s -> %s", region, production[region])

    return production



# TÂCHE 4 : ANALYSER LA CORRÉLATION


def analyser_correlation(**context):
    ti = context["ti"]

    donnees_meteo = ti.xcom_pull(task_ids="collecter_meteo_regions", key="return_value")
    donnees_production = ti.xcom_pull(task_ids="collecter_production_electrique", key="return_value")

    if donnees_meteo is None:
        raise ValueError("XCom météo introuvable pour collecter_meteo_regions")

    if donnees_production is None:
        raise ValueError("XCom production introuvable pour collecter_production_electrique")

    alertes = {}

    for region in REGIONS:
        meteo = donnees_meteo.get(region, {})
        production = donnees_production.get(region, {})

        alertes_region = []

        ensoleillement = float(meteo.get("ensoleillement_h", 0) or 0)
        vent = float(meteo.get("vent_kmh", 0) or 0)
        solaire = float(production.get("solaire_mw", 0) or 0)
        eolien = float(production.get("eolien_mw", 0) or 0)

        if ensoleillement > 6 and solaire <= 1000:
            alertes_region.append(
                f"ALERTE SOLAIRE : {ensoleillement:.1f}h de soleil mais seulement {solaire:.0f} MW produits"
            )

        if vent > 30 and eolien <= 2000:
            alertes_region.append(
                f"ALERTE ÉOLIEN : vent à {vent:.1f} km/h mais seulement {eolien:.0f} MW produits"
            )

        if solaire > 0 and ensoleillement == 0:
            alertes_region.append(
                f"ANOMALIE DONNÉES : {solaire:.0f} MW solaires produits sans ensoleillement enregistré"
            )

        alertes[region] = {
            "alertes": alertes_region,
            "ensoleillement_h": ensoleillement,
            "vent_kmh": vent,
            "solaire_mw": solaire,
            "eolien_mw": eolien,
            "statut": "ALERTE" if alertes_region else "OK",
        }

    return alertes



# TÂCHE 5 : GÉNÉRER LE RAPPORT FINAL


def generer_rapport_energie(**context):

    
    ti = context["ti"]
    analyse = ti.xcom_pull(task_ids="analyser_correlation", key="return_value") or {}
    today = date.today().isoformat()

    print("\n" + "=" * 80)
    print(f" RAPPORT ENERGIE & METEO — RTE — {today}")
    print("=" * 80)
    print(
        f"{'Region':<25} {'Soleil (h)':>10} {'Vent (km/h)':>12} "
        f"{'Solaire (MW)':>13} {'Eolien (MW)':>12} {'Statut':>8}"
    )
    print("-" * 80)

    for region, data in analyse.items():
        print(
            f"{region:<25} "
            f"{data['ensoleillement_h']:>10.1f} "
            f"{data['vent_kmh']:>12.1f} "
            f"{data['solaire_mw']:>13.0f} "
            f"{data['eolien_mw']:>12.0f} "
            f"{data['statut']:>8}"
        )

    print("=" * 80 + "\n")

    rapport = {
        "date": today,
        "source": "RTE eCO2mix + Open-Meteo",
        "pipeline": "energie_meteo_dag",
        "regions": analyse,
        "resume": {
            "nb_regions_analysees": len(analyse),
            "nb_alertes": sum(1 for r in analyse.values() if r["statut"] == "ALERTE"),
            "regions_en_alerte": [
                r for r, d in analyse.items() if d["statut"] == "ALERTE"
            ],
        },
    }

    chemin = f"/opt/airflow/logs/rapport_energie_{today}.json"

    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    logging.info(f"Rapport sauvegardé : {chemin}")
    return chemin


# Le DAG


with DAG(
    dag_id="energie_meteo_dag",  # nom unique du DAG dans Airflow
    default_args=default_args,   # paramètres par défaut des tâches
    description="Corrélation météo / production énergétique — RTE",
    schedule="0 6 * * *",        # tous les jours à 6h du matin
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),  # date de départ logique
    catchup=False,               # ne pas rejouer toutes les anciennes dates
    tags=["rte", "energie", "meteo", "open-data"],
) as dag:

    # Tâche 1 : vérifier les APIs
    t1 = PythonOperator(
        task_id="verifier_apis",
        python_callable=verifier_apis,
    )

    # Tâche 2 : collecter la météo
    t2 = PythonOperator(
        task_id="collecter_meteo_regions",
        python_callable=collecter_meteo_regions,
    )

    # Tâche 3 : collecter la production électrique
    t3 = PythonOperator(
        task_id="collecter_production_electrique",
        python_callable=collecter_production_electrique,
    )

    # Tâche 4 : analyser la corrélation météo / énergie
    t4 = PythonOperator(
        task_id="analyser_correlation",
        python_callable=analyser_correlation,
    )

    # Tâche 5 : générer le rapport final
    t5 = PythonOperator(
        task_id="generer_rapport_energie",
        python_callable=generer_rapport_energie,
    )

