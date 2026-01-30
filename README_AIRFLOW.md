# 🌪️ eBay Market Monitor - Airflow Pipeline

Ce projet contient un pipeline ETL automatisé avec **Apache Airflow** et **Docker**.
Il simule un flux de données quotidien pour alimenter le Dashboard Power BI de manière incrémentale.

## 🚀 Démarrage Rapide

### 1. Prérequis
- Docker Desktop installé et lancé (Baleine verte ✅).

### 2. Lancer le Pipeline
Ouvrez un terminal dans le dossier `airflow` et lancez :
```powershell
cd airflow
docker compose up -d
```
Cela lance 3 conteneurs (Scheduler, Webserver, Postgres).

### 3. Accéder à l'Interface
Ouvrez votre navigateur : **[http://localhost:8085](http://localhost:8085)**
- **User** : `admin`
- **Pass** : `admin`

Activez le DAG `ebay_morocco_etl_pipeline` (Toggle ON) pour qu'il se lance tous les jours à minuit.

---

## 🛑 Arrêter le Pipeline
Pour éteindre proprement les conteneurs (et libérer la mémoire) :
```powershell
docker compose down
```

---

## ⚙️ Comment ça marche ?

Le pipeline exécute 3 tâches tous les jours :

1.  **`simulate_arrival.py`** :
    - Lit **50,000 lignes** du fichier source `silver_data.csv`.
    - Simule une "nouveauté" quotidienne.
    - Durée du cycle avant répétition : ~460 jours.

2.  **`transform_gold.py`** :
    - Transforme ces 50,000 lignes en format **Star Schema**.
    - Calcule les agrégats (Fact Market & Fact Seller).

3.  **`load_postgres.py`** :
    - Insère (`INSERT`) les nouvelles données dans votre base PostgreSQL `ebay_maroc`.
    - Les données s'ajoutent à celles de la veille.

---

## 📊 Impact sur Power BI
- En mode **DirectQuery**, cliquez sur le bouton "Actualiser".
- Vous verrez les chiffres (Listings, Revenue) augmenter chaque jour.
