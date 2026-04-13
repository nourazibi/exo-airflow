# TP Noté Airflow — Data Platform Santé Publique ARS Occitanie

## Auteur
- Nom : Laazibi
- Prénom : Nour
- Formation : Master 2 Data Engineering
- Date : 12/04/2026

# TP Noté Airflow — Data Platform Santé Publique ARS Occitanie

## Auteur
- Nom : Laazibi
- Prénom : Nour
- Formation : Master 2 Data Engineering
- Date : 12/04/2026

## Prérequis
- Docker Desktop >= 4.0
- Docker Compose >= 2.0
- Python >= 3.11

## Structure du projet
```text
ars-epidemio/
├── dags/
│   ├── ars_epidemio_dag.py
│   └── sql/
│       └── init_ars_epidemio.sql
├── scripts/
│   ├── collecte_ias.py
│   └── calcul_indicateurs.py
├── docker-compose.yaml
├── .env.example
├── requirements.txt
├── README.md
└── output/
## Instructions de déploiement

### 1. Démarrage de la stack

Pour démarrer le projet, j’ai d’abord créé le fichier `.env` à la racine avec la variable `AIRFLOW_UID`.

Exemple de contenu du fichier `.env` :

```env
AIRFLOW_UID=50000

## Ensuite, j’ai lancé les commandes suivantes :

docker compose down -v
docker compose up -d
docker compose ps

## Ces commandes permettent :

d’arrêter et nettoyer les anciens conteneurs et volumes
de relancer toute la stack Docker Compose
de vérifier que tous les services sont bien démarrés

## Les services attendus sont :

postgres
postgres-ars
redis
airflow-webserver
airflow-scheduler
airflow-worker
flower

## J’ai ensuite vérifié l’accès aux interfaces :

Airflow UI : http://localhost:8080
Flower : http://localhost:5555

## Identifiants Airflow :

login : admin
mot de passe : admin


## 2. Configuration des connexions et variables Airflow

Après le démarrage de la stack, j’ai configuré Airflow depuis l’interface web.

Connexion Airflow

Dans Admin > Connections, j’ai créé la connexion suivante :

Conn Id : postgres_ars
Conn Type : Postgres
Host : postgres-ars
Schema : ars_epidemio
Login : postgres
Password : postgres
Port : 5432

Cette connexion permet au DAG d’écrire dans la base PostgreSQL métier.

**** Variables Airflow ***** 

Dans Admin > Variables, j’ai créé les variables suivantes :

semaines_historique = 12
seuil_alerte_incidence = 150
seuil_urgence_incidence = 500
seuil_alerte_zscore = 1.5
seuil_urgence_zscore = 3.0
departements_occitanie = ["09","11","12","30","31","32","34","46","48","65","66","81","82"]
syndromes_surveilles = ["GRIPPE","GEA","SG","BRONCHIO","COVID19"]
archive_base_path = /data/ars

Cette étape permet d’éviter d’écrire les paramètres directement dans le code et rend la configuration plus propre et plus flexible.

## 3. Démarrage du pipeline

Une fois Airflow configuré, j’ai lancé le pipeline depuis l’interface.

Étapes suivies :

ouvrir Airflow
repérer le DAG ars_epidemio_dag
activer le DAG
lancer un Run manuel
suivre l’exécution dans la vue Graph ou Grid
consulter les logs des tâches si nécessaire

Le DAG exécute les étapes suivantes :

vérification des connexions et variables
initialisation de la base PostgreSQL
collecte des données IAS
archivage des fichiers bruts
vérification de l’archive
calcul des indicateurs
insertion des données dans PostgreSQL
évaluation de la situation épidémique
génération du rapport JSON final

Le DAG contient aussi un branchement conditionnel :

declencher_alerte_ars
envoyer_bulletin_surveillance
confirmer_situation_normale

Dans mes exécutions, la situation détectée était NORMAL, donc la branche confirmer_situation_normale a été exécutée. Les deux autres branches ont été marquées comme skipped, ce qui est le comportement normal d’un BranchPythonOperator.

## Architecture des données

L’architecture repose sur un volume Docker persistant nommé ars-data.
Ce volume permet de conserver les données même après l’arrêt des conteneurs.

Le partitionnement choisi est le suivant :

/data/ars/raw/<annee>/<semaine>/
/data/ars/indicateurs/
/data/ars/rapports/<annee>/<semaine>/

Ce choix permet :

d’archiver les fichiers bruts par période
de retrouver facilement les données d’une semaine précise
de conserver les rapports JSON générés
de rendre le pipeline rejouable sur les semaines passées
Données IAS et région Occitanie

Les datasets IAS utilisent les anciens codes régions antérieurs à la réforme territoriale de 2016.
Il n’existe donc pas de colonne Loc_Reg76 dans les fichiers.

Pour obtenir l’indicateur Occitanie, j’ai calculé la moyenne de :

Loc_Reg91 : Languedoc-Roussillon
Loc_Reg73 : Midi-Pyrénées

La valeur IAS hebdomadaire de l’Occitanie est donc calculée à partir de la moyenne de ces deux colonnes.

**** Schéma PostgreSQL ****

La base ars_epidemio contient les tables principales suivantes :

syndromes
departements
donnees_hebdomadaires
indicateurs_epidemiques
rapports_ars

Ce schéma permet :

de stocker les données de référence
d’enregistrer les données hebdomadaires agrégées
de conserver les indicateurs calculés
d’archiver les rapports générés
d’assurer la traçabilité avec les colonnes created_at et updated_at
Décisions techniques

**** Plusieurs choix techniques ont été faits pour assurer le bon fonctionnement du projet. ****

J’ai utilisé l’image apache/airflow:2.8.0 afin de respecter la version demandée dans le sujet et de bénéficier d’un environnement stable.

Le mode CeleryExecutor a été choisi pour séparer l’orchestration et l’exécution des tâches. Cette architecture permet d’utiliser :

un webserver pour l’interface
un scheduler pour planifier les tâches
un worker pour exécuter les tâches
Flower pour superviser les workers

Redis a été utilisé comme broker Celery.
Deux bases PostgreSQL ont été séparées :

une pour les métadonnées Airflow
une pour les données métier du projet ARS

Le projet a été structuré de manière modulaire :

dags/ars_epidemio_dag.py pour le DAG principal
scripts/collecte_ias.py pour la collecte
scripts/calcul_indicateurs.py pour le calcul des indicateurs
dags/sql/init_ars_epidemio.sql pour le schéma SQL

Cette séparation rend le code plus lisible et plus maintenable.

Pour garantir l’idempotence, les insertions SQL utilisent ON CONFLICT DO UPDATE.
Ainsi, le pipeline peut être rejoué sur une même semaine sans créer de doublons.

Un volume Docker persistant a aussi été utilisé pour stocker les fichiers bruts et les rapports, ce qui est adapté à un cas de data engineering avec archivage.

Enfin, certaines variables supplémentaires ont été ajoutées dans docker-compose.yaml pour corriger l’affichage des logs dans l’interface Airflow et assurer le bon fonctionnement du worker.

**** Difficultés rencontrées et solutions ****

Plusieurs difficultés ont été rencontrées pendant la réalisation du TP.

## 1. Problème de permissions sur /data/ars

Au début, les tâches Airflow ne pouvaient pas écrire dans /data/ars/raw.
Le problème venait des droits sur le volume Docker.

Solution mise en place :

création automatique des dossiers /data/ars/raw, /data/ars/indicateurs et /data/ars/rapports
attribution des droits nécessaires dans le service airflow-init


## 2. Problème du conteneur airflow-init

Le conteneur airflow-init posait une erreur lorsque les commandes Airflow étaient lancées directement en root, avec un message indiquant que le module Airflow n’était pas trouvé.

Solution mise en place :

les opérations système ont été gardées en root pour créer les dossiers
les commandes Airflow ont été exécutées avec l’utilisateur airflow


## 3. Problème d’insertion PostgreSQL avec valeur_ias = null

Certaines semaines testées ne contenaient pas de données exploitables dans les datasets IAS.
Dans ce cas, valeur_ias était nulle, ce qui provoquait une erreur d’insertion dans PostgreSQL.

Solution mise en place :

ajout d’une vérification dans le DAG pour ignorer les enregistrements où valeur_ias est absente
génération du rapport maintenue même si aucune valeur exploitable n’est trouvée pour une semaine donnée


## 4. Problème d’affichage des logs dans l’interface Airflow

Au début, l’interface Airflow n’arrivait pas à afficher correctement les logs du worker.

Solution mise en place :

ajout de AIRFLOW__WEBSERVER__BASE_URL
ajout de AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT


## 5. Correction du sujet sur les codes régions IAS

Le sujet a été corrigé par l’enseignant concernant la région Occitanie.
Au départ, l’idée était d’utiliser Loc_Reg76, mais la version mise à jour précise qu’il faut utiliser les anciens codes régions et calculer l’Occitanie à partir de Loc_Reg91 et Loc_Reg73.

Solution mise en place :

mise à jour du script collecte_ias.py
remplacement de la logique Loc_Reg76 par une moyenne de Loc_Reg91 et Loc_Reg73
mise à jour du README et des explications pour rester conforme au sujet corrigé
Vérifications réalisées

**** Pour vérifier le bon fonctionnement du pipeline, j’ai utilisé plusieurs commandes.****

## Vérification des rapports générés

docker compose exec airflow-worker find /data/ars/rapports -type f | sort

## Vérification du contenu du volume

docker compose exec airflow-worker find /data/ars -type f | sort

docker compose exec airflow-worker du -sh /data/ars/

## Vérification en base PostgreSQL

docker compose exec postgres-ars psql -U postgres -d ars_epidemio -c "SELECT semaine, situation_globale, chemin_local FROM rapports_ars ORDER BY genere_le DESC;"

## Vérification des workers Celery

docker compose exec airflow-worker celery --app airflow.executors.celery_executor.app inspect active

docker compose exec airflow-worker celery --app airflow.executors.celery_executor.app inspect reserved

## Vérification des logs

docker compose logs airflow-scheduler --tail=50
docker compose logs -f airflow-worker
docker compose logs airflow-webserver --tail=20


**** Conclusion ****

Ce TP m’a permis de mettre en place une data platform complète sous Apache Airflow pour un cas d’usage de santé publique.

Le pipeline réalisé permet de :

collecter les données IAS
archiver les données brutes dans un volume Docker
calculer les indicateurs épidémiques
enregistrer les résultats dans PostgreSQL
choisir dynamiquement une branche d’exécution selon la situation
générer un rapport JSON hebdomadaire

Ce travail m’a permis de mieux comprendre :

l’orchestration de pipelines avec Airflow
le fonctionnement de CeleryExecutor
la gestion des volumes et permissions Docker
l’importance de l’idempotence
la séparation entre données techniques et données métier
la robustesse nécessaire dans un projet de data engineering


