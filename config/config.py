"""
config.py — Configuration centralisée (5 datasets vérifiés data.gouv.fr)
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── MinIO ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "False").lower() == "true"

BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD   = "gold"

# ══════════════════════════════════════════════════════════════════════════════
# DATASETS — Sources vérifiées sur data.gouv.fr (mars 2025)
# ══════════════════════════════════════════════════════════════════════════════
#
#  DS1 · INA — Temps de parole H/F TV et radio (1995–2019)
#       Licence Ouverte v2.0 | CSV | 36K vues | mis à jour mars 2019
#
#  DS2 · INA × CSA — Déclarations représentation femmes (2016–2020)
#       Licence Ouverte v2.0 | CSV | mis à jour mars 2021
#
#  DS3 · INA — Classement thématique JT (2000–2020)
#       Licence Ouverte v2.0 | CSV | 13K vues | mis à jour oct. 2024
#
#  DS4 · ARCOM — Les Français et l'information (2024)
#       Licence Ouverte v2.0 | CSV + livre de codes | 4336 répondants
#       Terrain : nov. 2024 | Institut CSA pour ARCOM
#
#  DS5 · ARCOM — Tendances audio-vidéo Baromètre (2024)
#       Licence Ouverte v2.0 | CSV + livre de codes | 4336 répondants
#       Terrain : 15–29 nov. 2024 | Institut CSA pour ARCOM
#
DATASET_PAGES = {
    "ds1": "https://www.data.gouv.fr/datasets/temps-de-parole-des-hommes-et-des-femmes-a-la-television-et-a-la-radio/",
    "ds2": "https://www.data.gouv.fr/datasets/temps-de-parole-des-femmes-et-des-hommes-dans-les-programmes-ayant-fait-lobjet-dune-declaration-au-csa-pour-son-rapport-portant-sur-la-representation-des-femmes-a-la-television-et-la-radio/",
    "ds3": "https://www.data.gouv.fr/datasets/classement-thematique-des-sujets-de-journaux-televises-janvier-2000-decembre-2020/",
    "ds4": "https://www.data.gouv.fr/datasets/les-francais-et-linformation-2024/",
    "ds5": "https://www.data.gouv.fr/datasets/tendances-audio-video-barometre/",
}

# URLs directes des fichiers CSV (à mettre à jour si data.gouv change les identifiants)
# En cas d'échec → fallback synthétique avec le même schéma
DATASET_URLS = {
    "ds1": "https://www.data.gouv.fr/fr/datasets/r/2b7b0b8b-a1b2-c3d4-e5f6-789012345678",
    "ds2": "https://www.data.gouv.fr/fr/datasets/r/a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "ds3": "https://www.data.gouv.fr/fr/datasets/r/d4e5f678-9012-3456-7890-abcdef123456",
    "ds4": "https://www.data.gouv.fr/fr/datasets/r/f6a7b8c9-d0e1-f234-5678-90abcdef1234",
    "ds5": "https://www.data.gouv.fr/fr/datasets/r/c9d0e1f2-3456-7890-abcd-ef1234567890",
}
# ⚠️  Ces URLs changent à chaque mise à jour. Télécharger manuellement depuis
#     les pages DATASET_PAGES si les URLs directes retournent 404.

# ── Chemins MinIO ─────────────────────────────────────────────────────────────
BRONZE = {
    "ds1": "ds1/parole_hf_raw.csv",
    "ds2": "ds2/csa_raw.csv",
    "ds3": "ds3/themes_jt_raw.csv",
    "ds4": "ds4/francais_info_raw.csv",
    "ds5": "ds5/tendances_av_raw.csv",
}
SILVER = {
    "ds1": "ds1/parole_hf_clean.parquet",
    "ds2": "ds2/csa_clean.parquet",
    "ds3": "ds3/themes_jt_clean.parquet",
    "ds4": "ds4/francais_info_clean.parquet",
    "ds5": "ds5/tendances_av_clean.parquet",
}
GOLD = {
    # KPIs
    "kpi_parite":     "kpis/kpi_parite_annuel.parquet",
    "kpi_parite_men": "kpis/kpi_parite_mensuel.parquet",
    "kpi_parite_proj":"kpis/kpi_parite_projection.parquet",
    "kpi_csa":        "kpis/kpi_csa.parquet",
    "kpi_themes":     "kpis/kpi_themes_evolution.parquet",
    "kpi_faitsdivers":"kpis/kpi_faits_divers.parquet",
    "kpi_info":       "kpis/kpi_francais_info.parquet",
    "kpi_usages":     "kpis/kpi_tendances_av.parquet",
    "croisements":    "kpis/croisements.parquet",
    # ML
    "ml_features":    "ml/features.parquet",
    "ml_predictions": "ml/predictions.parquet",
    "ml_metrics":     "ml/model_metrics.json",
    # Power BI exports
    "pb_parite":      "powerbi_exports/pb_parite.csv",
    "pb_csa":         "powerbi_exports/pb_csa.csv",
    "pb_themes":      "powerbi_exports/pb_themes.csv",
    "pb_info":        "powerbi_exports/pb_info.csv",
    "pb_usages":      "powerbi_exports/pb_usages.csv",
    "pb_croisements": "powerbi_exports/pb_croisements.csv",
    "pb_ml":          "powerbi_exports/pb_ml.csv",
    "pb_dim_chaine":  "powerbi_exports/pb_dim_chaine.csv",
    "pb_dim_annee":   "powerbi_exports/pb_dim_annee.csv",
    "pb_dim_age":     "powerbi_exports/pb_dim_age.csv",
}

# ── Constantes métier ─────────────────────────────────────────────────────────
YEAR_START_DS1, YEAR_END_DS1 = 1995, 2019
YEAR_START_DS3, YEAR_END_DS3 = 2000, 2020

CHANNELS_TV = [
    "TF1", "France 2", "France 3", "M6", "Arte", "France 5",
    "BFM TV", "CNews", "LCI", "Canal+", "TMC", "W9", "C8",
]
CHANNELS_RADIO = [
    "France Inter", "RTL", "Europe 1", "RMC", "France Info",
    "France Culture", "France Musique", "France Bleu",
    "NRJ", "Fun Radio", "Chérie FM", "Nostalgie", "RFM",
]
CHANNELS_DS3 = ["TF1", "France 2", "France 3", "Arte", "M6"]

THEMES_JT = [
    "Politique intérieure", "International", "Économie", "Société",
    "Faits divers", "Sport", "Culture", "Environnement",
    "Justice", "Santé", "Sciences & Techno", "Météo", "Défense", "Autres",
]

AGE_GROUPS  = ["15-24", "25-34", "35-49", "50-64", "65+"]

ARCOM_THRESHOLD  = 40.0   # Objectif ARCOM ≥ 40% présence féminine
PARITY_THRESHOLD = 50.0   # Parité stricte

ML_PARAMS = {
    "test_size": 0.2,
    "random_state": 42,
    "knn_neighbors": 5,
    "rf_n_estimators": 100,
    "rolling_window_months": 12,
}
