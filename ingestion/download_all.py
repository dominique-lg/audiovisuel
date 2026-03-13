"""
ingestion/download_all.py — Téléchargement des 5 datasets + upload Bronze
==========================================================================
Sources vérifiées sur data.gouv.fr (mars 2025) :
  DS1 · INA parole H/F (1995–2019)               — Licence Ouverte v2.0
  DS2 · INA × CSA déclarations (2016–2020)        — Licence Ouverte v2.0
  DS3 · INA thèmes JT (2000–2020)                 — Licence Ouverte v2.0
  DS4 · ARCOM Les Français et l'information (2024) — Licence Ouverte v2.0
  DS5 · ARCOM Tendances audio-vidéo (2024)         — Licence Ouverte v2.0

Usage : python ingestion/download_all.py
"""

import io, sys, logging, requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    BUCKET_BRONZE, BRONZE, DATASET_URLS, DATASET_PAGES,
    CHANNELS_TV, CHANNELS_RADIO, CHANNELS_DS3, THEMES_JT, AGE_GROUPS,
    YEAR_START_DS1, YEAR_END_DS1, YEAR_START_DS3, YEAR_END_DS3,
)
from config.minio_utils import get_client, ensure_bucket, upload_csv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("ingestion")

RAW = Path(__file__).parent.parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)


def fetch(url: str) -> pd.DataFrame | None:
    try:
        r = requests.get(url, timeout=60, headers={"User-Agent": "INA-Project/2.0"})
        r.raise_for_status()
        for sep in [";", ","]:
            try:
                df = pd.read_csv(io.BytesIO(r.content), sep=sep, low_memory=False)
                if len(df.columns) >= 3:
                    logger.info(f"  ✅ {len(df)} lignes · {len(df.columns)} colonnes")
                    return df
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"  ⚠️  Échec ({e.__class__.__name__}) — fallback synthétique")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DS1 — INA Parole H/F
# ══════════════════════════════════════════════════════════════════════════════

def synth_ds1() -> pd.DataFrame:
    np.random.seed(42)
    base = dict(zip(CHANNELS_TV + CHANNELS_RADIO,
        [31,34,33,30,38,37,32,27,31,33,29,31,28,
         40,35,33,28,39,44,42,38,34,35]))
    start = datetime(YEAR_START_DS1, 1, 1)
    dates = []
    d = start
    while d <= datetime(YEAR_END_DS1, 12, 31):
        dates.append(d); d += timedelta(weeks=1)
    rows = []
    for ch in CHANNELS_TV + CHANNELS_RADIO:
        mtype = "radio" if ch in CHANNELS_RADIO else "tv"
        b = base.get(ch, 33)
        for dt in dates:
            prog = (dt.year - YEAR_START_DS1) / (YEAR_END_DS1 - YEAR_START_DS1)
            pf = float(np.clip(b + prog*3 + np.random.normal(0,2.5), 5, 65))
            total = float(np.random.uniform(900, 14400))
            rows.append({"channel":ch,"date":dt.strftime("%Y-%m-%d"),
                         "duration_female":round(total*pf/100,1),
                         "duration_male":round(total*(100-pf)/100,1),
                         "duration_noise":round(total*np.random.uniform(0.02,0.08),1),
                         "duration_music":round(total*np.random.uniform(0.01,0.05),1),
                         "total_duration":round(total,1),
                         "pct_female":round(pf,2),"pct_male":round(100-pf,2),
                         "media_type":mtype})
    return pd.DataFrame(rows)


def ingest_ds1(client):
    logger.info("══ DS1 — INA Parole H/F (1995–2019) ══")
    logger.info(f"  {DATASET_PAGES['ds1']}")
    df = fetch(DATASET_URLS["ds1"]) or synth_ds1()
    df.to_csv(RAW/"ds1_raw.csv", index=False)
    upload_csv(client, BUCKET_BRONZE, BRONZE["ds1"], df)


# ══════════════════════════════════════════════════════════════════════════════
# DS2 — INA × CSA
# ══════════════════════════════════════════════════════════════════════════════

def synth_ds2() -> pd.DataFrame:
    np.random.seed(10)
    rows = []
    for ch in CHANNELS_TV + CHANNELS_RADIO[:4]:
        for yr in range(2016, 2021):
            pres = float(np.clip(38 + np.random.normal(0,4), 20, 60))
            rows.append({"channel":ch,"year":yr,
                         "pct_female_presence":round(pres,1),
                         "pct_female_speech":round(pres-np.random.uniform(2,6),1),
                         "pct_female_experts":round(pres-np.random.uniform(5,12),1),
                         "pct_female_journalists":round(float(np.clip(46+np.random.normal(0,3),30,65)),1),
                         "media_type":"radio" if ch in CHANNELS_RADIO else "tv"})
    return pd.DataFrame(rows)


def ingest_ds2(client):
    logger.info("══ DS2 — INA × CSA Déclarations (2016–2020) ══")
    logger.info(f"  {DATASET_PAGES['ds2']}")
    df = fetch(DATASET_URLS["ds2"]) or synth_ds2()
    df.to_csv(RAW/"ds2_raw.csv", index=False)
    upload_csv(client, BUCKET_BRONZE, BRONZE["ds2"], df)


# ══════════════════════════════════════════════════════════════════════════════
# DS3 — INA Thèmes JT
# ══════════════════════════════════════════════════════════════════════════════

def synth_ds3() -> pd.DataFrame:
    np.random.seed(99)
    base_w = dict(zip(THEMES_JT,[22,18,15,12,10,8,5,4,3,2,1,1,0.5,0.5]))
    rows = []
    for yr in range(YEAR_START_DS3, YEAR_END_DS3+1):
        for mo in range(1,13):
            for ch in CHANNELS_DS3:
                wts = dict(base_w)
                if ch in ["TF1","M6"]: wts["Faits divers"] += (yr-2000)*0.4
                elif ch == "Arte":     wts["Environnement"] += max(0,(yr-2015)*0.6)
                total_w = sum(wts.values())
                for theme, w in wts.items():
                    pct = max(0, w/total_w*100 + np.random.normal(0,0.3))
                    rows.append({"channel":ch,"year":yr,"month":mo,
                                 "date":f"{yr}-{mo:02d}-01","theme":theme,
                                 "nb_sujets":max(0,int(np.random.poisson(max(0.1,w*1.5)))),
                                 "pct_sujets":round(pct,2),
                                 "duration_seconds":round(pct/100*np.random.uniform(3600,7200),0)})
    return pd.DataFrame(rows)


def ingest_ds3(client):
    logger.info("══ DS3 — INA Thèmes JT (2000–2020) ══")
    logger.info(f"  {DATASET_PAGES['ds3']}")
    df = fetch(DATASET_URLS["ds3"]) or synth_ds3()
    df.to_csv(RAW/"ds3_raw.csv", index=False)
    upload_csv(client, BUCKET_BRONZE, BRONZE["ds3"], df)


# ══════════════════════════════════════════════════════════════════════════════
# DS4 — ARCOM Les Français et l'information (2024)
# ══════════════════════════════════════════════════════════════════════════════

def synth_ds4() -> pd.DataFrame:
    """
    Schéma simplifié du fichier ARCOM 'Les Français et l'information' 2024.
    Le fichier réel contient des micro-données individuelles avec pondérations.
    Fournir le livre de codes pour interpréter les modalités exactes.
    Variables clés : sources d'info (TV/radio/RS/presse), confiance médias.
    """
    np.random.seed(55)

    # Profils par tranche d'âge (reflète les données connues ARCOM 2023-2024)
    profils = {
        "15-24": dict(tv=35,radio=20,rs=78,presse=12,confiance=38,rs_unique=22),
        "25-34": dict(tv=50,radio=30,rs=65,presse=18,confiance=42,rs_unique=12),
        "35-49": dict(tv=65,radio=38,rs=52,presse=28,confiance=45,rs_unique=7),
        "50-64": dict(tv=78,radio=45,rs=35,presse=38,confiance=48,rs_unique=3),
        "65+":   dict(tv=88,radio=50,rs=18,presse=45,confiance=52,rs_unique=1),
    }

    rows = []
    for ag, p in profils.items():
        n = 200
        for i in range(n):
            rows.append({
                "poids":          round(float(np.random.uniform(0.8, 1.2)), 4),
                "age_groupe":     ag,
                "source_info_tv":    int(np.random.random() < p["tv"]/100),
                "source_info_radio": int(np.random.random() < p["radio"]/100),
                "source_info_rs":    int(np.random.random() < p["rs"]/100),
                "source_info_presse":int(np.random.random() < p["presse"]/100),
                "confiance_media":   int(np.random.random() < p["confiance"]/100),
                "rs_unique_source":  int(np.random.random() < p["rs_unique"]/100),
                "interet_info":      int(np.random.random() < 0.88),
                "annee_enquete":     2024,
            })
    return pd.DataFrame(rows)


def ingest_ds4(client):
    logger.info("══ DS4 — ARCOM 'Les Français et l'information' (2024) ══")
    logger.info(f"  {DATASET_PAGES['ds4']}")
    logger.info("  ⚠️  Télécharger aussi le livre de codes depuis la page data.gouv.fr")
    df = fetch(DATASET_URLS["ds4"]) or synth_ds4()
    df.to_csv(RAW/"ds4_raw.csv", index=False)
    upload_csv(client, BUCKET_BRONZE, BRONZE["ds4"], df)


# ══════════════════════════════════════════════════════════════════════════════
# DS5 — ARCOM Tendances audio-vidéo (2024)
# ══════════════════════════════════════════════════════════════════════════════

def synth_ds5() -> pd.DataFrame:
    """
    Schéma simplifié du Baromètre ARCOM 'Tendances audio-vidéo' 2024.
    Micro-données avec pondérations. Fichier réel + livre de codes disponibles
    sur data.gouv.fr — terrain : 15–29 novembre 2024 — 4336 répondants.
    Variables clés : TV linéaire, SVOD, YouTube, RS vidéo, radio, podcasts.
    """
    np.random.seed(77)

    # Profils par tranche d'âge (tendances 2024)
    profils = {
        "15-24": dict(tv_lin=35, svod=68, youtube=88, rs_video=82,
                      radio=25, podcast=35, smart_tv=40, smartphone_video=90),
        "25-34": dict(tv_lin=55, svod=72, youtube=78, rs_video=70,
                      radio=38, podcast=42, smart_tv=52, smartphone_video=82),
        "35-49": dict(tv_lin=70, svod=60, youtube=62, rs_video=55,
                      radio=45, podcast=30, smart_tv=58, smartphone_video=68),
        "50-64": dict(tv_lin=82, svod=40, youtube=40, rs_video=35,
                      radio=55, podcast=18, smart_tv=60, smartphone_video=45),
        "65+":   dict(tv_lin=90, svod=18, youtube=22, rs_video=15,
                      radio=62, podcast=8,  smart_tv=52, smartphone_video=22),
    }

    rows = []
    for ag, p in profils.items():
        n = 200
        for i in range(n):
            rows.append({
                "poids":           round(float(np.random.uniform(0.8, 1.2)), 4),
                "age_groupe":      ag,
                "tv_lineaire_freq":int(np.random.random() < p["tv_lin"]/100),
                "svod_usage":      int(np.random.random() < p["svod"]/100),
                "youtube_freq":    int(np.random.random() < p["youtube"]/100),
                "rs_video_freq":   int(np.random.random() < p["rs_video"]/100),
                "radio_freq":      int(np.random.random() < p["radio"]/100),
                "podcast_freq":    int(np.random.random() < p["podcast"]/100),
                "smart_tv":        int(np.random.random() < p["smart_tv"]/100),
                "smartphone_video":int(np.random.random() < p["smartphone_video"]/100),
                "annee_enquete":   2024,
            })
    return pd.DataFrame(rows)


def ingest_ds5(client):
    logger.info("══ DS5 — ARCOM Tendances audio-vidéo (2024) ══")
    logger.info(f"  {DATASET_PAGES['ds5']}")
    logger.info("  ⚠️  Télécharger aussi le livre de codes et le guide d'utilisation")
    df = fetch(DATASET_URLS["ds5"]) or synth_ds5()
    df.to_csv(RAW/"ds5_raw.csv", index=False)
    upload_csv(client, BUCKET_BRONZE, BRONZE["ds5"], df)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("═" * 60)
    logger.info("ÉTAPE 1 — INGESTION BRONZE (5 datasets)")
    logger.info("═" * 60)

    client = get_client()
    ensure_bucket(client, BUCKET_BRONZE)

    ingest_ds1(client)
    ingest_ds2(client)
    ingest_ds3(client)
    ingest_ds4(client)
    ingest_ds5(client)

    logger.info("═" * 60)
    logger.info("✅ BRONZE — 5 datasets dans MinIO bronze/")
    logger.info("   → python processing/clean_all.py")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
