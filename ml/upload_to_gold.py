"""ml/upload_to_gold.py — Upload Gold + génération des 10 exports Power BI"""
import sys, json, logging
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    BUCKET_GOLD, ARCOM_THRESHOLD, PARITY_THRESHOLD,
    CHANNELS_TV, CHANNELS_RADIO, CHANNELS_DS3,
)
from config.minio_utils import get_client, ensure_bucket, upload_parquet, upload_csv, upload_json

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("upload_gold")

GOLD_DIR = Path(__file__).parent.parent / "data" / "gold"


def load(name):
    p = GOLD_DIR / f"{name}.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def build_exports():
    exports = {}

    # ── Page 1 : Parité ──────────────────────────────────────────────────────
    ann  = load("kpi_parite_annuel")
    proj = load("kpi_parite_projection")
    pb = ann.merge(proj[["channel","slope_annuel","r2","annee_parite_50"]],
                   on="channel", how="left") if len(proj) else ann
    pb["objectif_arcom"]  = ARCOM_THRESHOLD
    pb["objectif_parite"] = PARITY_THRESHOLD
    exports["pb_parite"] = pb
    exports["pb_csa"]    = load("kpi_csa")

    # ── Page 2 : Thèmes JT ───────────────────────────────────────────────────
    exports["pb_themes"]      = load("kpi_themes_evolution")
    exports["pb_faits_divers"] = load("kpi_faits_divers")

    # ── Page 3 : Sources d'info ───────────────────────────────────────────────
    exports["pb_info"] = load("kpi_francais_info")

    # ── Page 4 : Usages & écrans ──────────────────────────────────────────────
    exports["pb_usages"] = load("kpi_tendances_av")

    # ── Page 5 : Croisements & ML ─────────────────────────────────────────────
    cr_parts = []
    for name in ["croisement_ds1_ds3","croisement_ds1_ds2","croisement_ds4_ds5"]:
        df = load(name)
        if len(df): cr_parts.append(df.assign(croisement=name))
    if cr_parts:
        exports["pb_croisements"] = pd.concat(cr_parts, ignore_index=True)
    exports["pb_correlations"] = load("corr_parite_faitsdiv")

    # ML
    m_path = GOLD_DIR / "model_metrics.json"
    if m_path.exists():
        with open(m_path) as f: metrics = json.load(f)
        ml_rows = [{"model":m,"accuracy":metrics[m]["accuracy"],
                    "f1_macro":metrics[m]["f1_macro"],
                    "cv_f1_mean":metrics[m]["cv_f1_mean"],
                    "cv_f1_std":metrics[m]["cv_f1_std"],
                    "is_best":m==metrics.get("best_model")}
                   for m in ["knn","random_forest"] if m in metrics]
        fi = metrics.get("random_forest",{}).get("feature_importances",{})
        exports["pb_ml_metrics"]     = pd.DataFrame(ml_rows)
        exports["pb_ml_importances"] = pd.DataFrame(
            [{"feature":k,"importance":v} for k,v in sorted(fi.items(),key=lambda x:-x[1])])

    # ── Dimensions ────────────────────────────────────────────────────────────
    channels = list(set(CHANNELS_TV + CHANNELS_RADIO))
    groupe = {
        "TF1":"Groupe TF1","France 2":"France Télévisions",
        "France 3":"France Télévisions","M6":"Groupe M6","Arte":"Arte GEIE",
        "France 5":"France Télévisions","BFM TV":"Altice Media",
        "CNews":"Groupe Canal+","LCI":"Groupe TF1",
        "France Inter":"Radio France","RTL":"Groupe RTL",
        "Europe 1":"Lagardère","RMC":"Altice Media","France Info":"Radio France",
        "France Culture":"Radio France","France Musique":"Radio France",
        "France Bleu":"Radio France",
    }
    sp_pub = {"France 2","France 3","France 5","Arte","France Inter",
              "France Info","France Culture","France Musique","France Bleu"}
    exports["pb_dim_chaine"] = pd.DataFrame({
        "channel":channels,
        "media_type":["radio" if c in CHANNELS_RADIO else "tv" for c in channels],
        "groupe":[groupe.get(c,"Autre") for c in channels],
        "service_public":[c in sp_pub for c in channels],
        "in_ds3":[c in CHANNELS_DS3 for c in channels],
    })

    years = list(range(1995, 2025))
    periode = {y:"#MeToo & parité" if y>=2017 else "Montée info continue" if y>=2010
               else "Présidentielles 2002–2007" if y>=2002 else "Ère pré-numérique"
               for y in years}
    events = {2002:"Présidentielle",2007:"Présidentielle",2012:"Présidentielle",
              2015:"Attentats Paris",2017:"#MeToo",2018:"Gilets Jaunes",
              2020:"COVID-19",2022:"Guerre Ukraine"}
    exports["pb_dim_annee"] = pd.DataFrame({
        "year":years,
        "periode":[periode[y] for y in years],
        "evenement":[events.get(y,"") for y in years],
        "in_ds1":[1995<=y<=2019 for y in years],
        "in_ds3":[2000<=y<=2020 for y in years],
    })

    exports["pb_dim_age"] = pd.DataFrame({
        "age_groupe":["15-24","25-34","35-49","50-64","65+"],
        "label_court":["15–24 ans","25–34 ans","35–49 ans","50–64 ans","65 ans et +"],
        "generation":["Gen Z","Millennials","Gen X","Baby-boomers","Seniors"],
    })

    return exports


def main():
    logger.info("═" * 60)
    logger.info("ÉTAPE 3 — UPLOAD GOLD + EXPORTS POWER BI")
    logger.info("═" * 60)

    client = get_client()
    ensure_bucket(client, BUCKET_GOLD)

    # Upload Parquet Gold
    for name in ["kpi_parite_annuel","kpi_parite_mensuel","kpi_parite_projection",
                 "kpi_csa","kpi_themes_evolution","kpi_faits_divers","kpi_heatmap_themes",
                 "kpi_francais_info","kpi_tendances_av",
                 "croisement_ds1_ds3","croisement_ds1_ds2","croisement_ds4_ds5",
                 "corr_parite_faitsdiv","ml_features","ml_predictions"]:
        p = GOLD_DIR / f"{name}.parquet"
        if p.exists():
            upload_parquet(client, BUCKET_GOLD, f"kpis/{name}.parquet", pd.read_parquet(p))

    m = GOLD_DIR / "model_metrics.json"
    if m.exists():
        with open(m) as f: upload_json(client, BUCKET_GOLD, "ml/model_metrics.json", json.load(f))

    # Exports Power BI
    exports = build_exports()
    for name, df in exports.items():
        if len(df):
            upload_csv(client, BUCKET_GOLD, f"powerbi_exports/{name}.csv", df)

    logger.info("═" * 60)
    logger.info("✅ GOLD COMPLET")
    logger.info("")
    logger.info("  Parquet KPIs → gold/kpis/")
    logger.info("  Exports Power BI → gold/powerbi_exports/ :")
    logger.info("    Page 1 Parité   : pb_parite.csv · pb_csa.csv")
    logger.info("    Page 2 Thèmes   : pb_themes.csv · pb_faits_divers.csv")
    logger.info("    Page 3 Info     : pb_info.csv")
    logger.info("    Page 4 Usages   : pb_usages.csv")
    logger.info("    Page 5 Croix+ML : pb_croisements.csv · pb_correlations.csv")
    logger.info("                      pb_ml_metrics.csv · pb_ml_importances.csv")
    logger.info("    Dims            : pb_dim_chaine · pb_dim_annee · pb_dim_age")
    logger.info("")
    logger.info("  → Voir powerbi/README_powerbi.md")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
