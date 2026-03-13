"""
processing/clean_all.py — Nettoyage des 5 datasets → couche Silver
===================================================================
Usage : python processing/clean_all.py
"""

import sys, logging
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    BUCKET_BRONZE, BUCKET_SILVER, BRONZE, SILVER,
    CHANNELS_RADIO, YEAR_START_DS1, YEAR_END_DS1,
    YEAR_START_DS3, YEAR_END_DS3, ARCOM_THRESHOLD,
)
from config.minio_utils import get_client, ensure_bucket, download_csv, upload_parquet

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("clean_all")

SILVER_DIR = Path(__file__).parent.parent / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)


def snake(df):
    df.columns = (df.columns.str.strip().str.lower()
                  .str.replace(r"[\s\-./]","_",regex=True)
                  .str.replace(r"[^\w]","",regex=True))
    return df

def diag(df, label):
    logger.info(f"  📊 {label} → {df.shape[0]} × {df.shape[1]}")


# ── DS1 ───────────────────────────────────────────────────────────────────────

def clean_ds1(client):
    logger.info("══ DS1 — INA Parole H/F ══")
    df = download_csv(client, BUCKET_BRONZE, BRONZE["ds1"])
    diag(df, "Bronze"); df = snake(df)

    aliases = {"chaine":"channel","station":"channel","date_diffusion":"date",
               "duree_female":"duration_female","duree_male":"duration_male",
               "taux_female":"pct_female","taux_male":"pct_male","type_media":"media_type"}
    df = df.rename(columns={k:v for k,v in aliases.items() if k in df.columns})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["channel","date"])

    for c in ["duration_female","duration_male","duration_noise","duration_music",
              "total_duration","pct_female","pct_male"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",","."),errors="coerce").fillna(0).clip(0)

    n = len(df)
    df = df.drop_duplicates(subset=["channel","date"], keep="last")
    logger.info(f"  Doublons : {n-len(df)}")

    speech = df.get("duration_female", pd.Series(0,index=df.index)) \
             + df.get("duration_male",   pd.Series(0,index=df.index))
    recalc = (speech > 0) & (
        df.get("pct_female", pd.Series(np.nan)).isna() |
        ((df.get("pct_female",pd.Series(0)) + df.get("pct_male",pd.Series(0))).sub(100).abs() > 1))
    if recalc.any():
        df.loc[recalc,"pct_female"] = (df.loc[recalc,"duration_female"]/speech[recalc]*100).round(2)
        df.loc[recalc,"pct_male"]   = (df.loc[recalc,"duration_male"]  /speech[recalc]*100).round(2)
    df["pct_female"] = df["pct_female"].clip(0,100)
    df["pct_male"]   = df["pct_male"].clip(0,100)

    if "media_type" not in df.columns or df["media_type"].isna().all():
        df["media_type"] = df["channel"].apply(lambda c: "radio" if c in CHANNELS_RADIO else "tv")

    df = df[df["date"].dt.year.between(YEAR_START_DS1, YEAR_END_DS1)]
    df["year"]         = df["date"].dt.year
    df["month"]        = df["date"].dt.month
    df["year_month"]   = df["date"].dt.to_period("M").astype(str)
    df["ecart_parite"] = (50 - df["pct_female"]).round(2)
    df["above_arcom"]  = (df["pct_female"] >= ARCOM_THRESHOLD).astype(int)
    df["silver_at"]    = datetime.now().isoformat()

    diag(df, "Silver")
    return df.sort_values(["channel","date"]).reset_index(drop=True)


# ── DS2 ───────────────────────────────────────────────────────────────────────

def clean_ds2(client):
    logger.info("══ DS2 — INA × CSA ══")
    df = download_csv(client, BUCKET_BRONZE, BRONZE["ds2"])
    diag(df, "Bronze"); df = snake(df)

    if "annee" in df.columns and "year" not in df.columns:
        df = df.rename(columns={"annee":"year"})
    df["year"] = pd.to_numeric(df.get("year", 2019), errors="coerce").fillna(2019).astype(int)

    for c in [col for col in df.columns if "pct" in col or "taux" in col]:
        df[c] = pd.to_numeric(df[c].astype(str).str.replace(",",".").str.replace("%",""),
                              errors="coerce").clip(0,100)

    if "channel" not in df.columns and "chaine" in df.columns:
        df = df.rename(columns={"chaine":"channel"})

    df = df.drop_duplicates(subset=["channel","year"] if "channel" in df.columns else None, keep="last")

    if "media_type" not in df.columns:
        df["media_type"] = df["channel"].apply(lambda c: "radio" if c in CHANNELS_RADIO else "tv")

    if {"pct_female_presence","pct_female_speech"}.issubset(df.columns):
        df["delta_presence_parole"] = (df["pct_female_presence"] - df["pct_female_speech"]).round(2)

    df["above_arcom"] = (df.get("pct_female_presence", pd.Series(0)) >= ARCOM_THRESHOLD).astype(int)
    df["silver_at"]   = datetime.now().isoformat()
    diag(df, "Silver"); return df


# ── DS3 ───────────────────────────────────────────────────────────────────────

def clean_ds3(client):
    logger.info("══ DS3 — INA Thèmes JT ══")
    df = download_csv(client, BUCKET_BRONZE, BRONZE["ds3"])
    diag(df, "Bronze"); df = snake(df)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df["year"]  = df["date"].dt.year
        df["month"] = df["date"].dt.month

    for c in ["nb_sujets","pct_sujets","duration_seconds"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",","."),errors="coerce").fillna(0).clip(0)

    key = [c for c in ["channel","year","month","theme"] if c in df.columns]
    n = len(df)
    df = df.drop_duplicates(subset=key, keep="last")
    logger.info(f"  Doublons : {n-len(df)}")

    if "pct_sujets" in df.columns:
        s = df.groupby(["channel","year","month"])["pct_sujets"].transform("sum")
        df["pct_sujets_norm"] = (df["pct_sujets"]/s.replace(0,np.nan)*100).round(2).fillna(0)
        df["rang_theme"] = df.groupby(["channel","year","month"])["pct_sujets"].rank(
            ascending=False, method="dense").astype(int)

    df = df[df["year"].between(YEAR_START_DS3, YEAR_END_DS3)]
    df["silver_at"] = datetime.now().isoformat()
    diag(df, "Silver"); return df


# ── DS4 — ARCOM Les Français et l'information ──────────────────────────────────

def clean_ds4(client):
    logger.info("══ DS4 — ARCOM Les Français et l'information (2024) ══")
    logger.info("  ℹ️  Appliquer les pondérations (poids) — voir livre de codes")
    df = download_csv(client, BUCKET_BRONZE, BRONZE["ds4"])
    diag(df, "Bronze"); df = snake(df)

    # Harmonisation noms colonnes (variables du fichier ARCOM)
    aliases = {
        "poids03":"poids", "poids_ind":"poids", "pond":"poids",
        "age":"age_groupe", "q_age":"age_groupe", "tranche_age":"age_groupe",
    }
    df = df.rename(columns={k:v for k,v in aliases.items() if k in df.columns})

    if "poids" in df.columns:
        df["poids"] = pd.to_numeric(df["poids"], errors="coerce").fillna(1.0).clip(0)

    # Variables binaires (0/1 ou modalités)
    bin_cols = [c for c in df.columns if any(k in c for k in
                ["source","confiance","rs_unique","interet","usage","freq","tv_","radio_",
                 "podcast","svod","smart","smartphone","youtube","presse"])]
    for c in bin_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates()
    df["silver_at"] = datetime.now().isoformat()
    diag(df, "Silver"); return df


# ── DS5 — ARCOM Tendances audio-vidéo ────────────────────────────────────────

def clean_ds5(client):
    logger.info("══ DS5 — ARCOM Tendances audio-vidéo (2024) ══")
    logger.info("  ℹ️  Appliquer les pondérations (poids) — voir livre de codes + guide")
    df = download_csv(client, BUCKET_BRONZE, BRONZE["ds5"])
    diag(df, "Bronze"); df = snake(df)

    aliases = {"poids03":"poids","poids_ind":"poids","age":"age_groupe","q_age":"age_groupe"}
    df = df.rename(columns={k:v for k,v in aliases.items() if k in df.columns})

    if "poids" in df.columns:
        df["poids"] = pd.to_numeric(df["poids"], errors="coerce").fillna(1.0).clip(0)

    num_cols = [c for c in df.columns if any(k in c for k in
                ["tv_lin","svod","youtube","rs_video","radio","podcast",
                 "smart","smartphone","freq","usage","duree"])]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates()
    df["silver_at"] = datetime.now().isoformat()
    diag(df, "Silver"); return df


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("═" * 60)
    logger.info("ÉTAPE 2 — NETTOYAGE SILVER (5 datasets)")
    logger.info("═" * 60)

    client = get_client()
    ensure_bucket(client, BUCKET_SILVER)

    steps = [
        ("ds1", clean_ds1, SILVER["ds1"]),
        ("ds2", clean_ds2, SILVER["ds2"]),
        ("ds3", clean_ds3, SILVER["ds3"]),
        ("ds4", clean_ds4, SILVER["ds4"]),
        ("ds5", clean_ds5, SILVER["ds5"]),
    ]
    for name, cleaner, path in steps:
        df = cleaner(client)
        df.to_parquet(SILVER_DIR/f"{name}_clean.parquet", index=False, engine="pyarrow")
        upload_parquet(client, BUCKET_SILVER, path, df)

    logger.info("═" * 60)
    logger.info("✅ SILVER — 5 datasets nettoyés")
    logger.info("   → python ml/features.py")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
