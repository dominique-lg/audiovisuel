"""
ml/features.py — KPIs + croisements inter-datasets (5 datasets) → Gold
=======================================================================
Usage : python ml/features.py
"""

import sys, logging
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats as sp

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    BUCKET_SILVER, SILVER, CHANNELS_DS3,
    ARCOM_THRESHOLD, PARITY_THRESHOLD, ML_PARAMS,
)
from config.minio_utils import get_client, download_parquet

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("features")

GOLD_DIR = Path(__file__).parent.parent / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)


def save(df, name):
    p = GOLD_DIR / f"{name}.parquet"
    df.to_parquet(p, index=False, engine="pyarrow")
    logger.info(f"  💾 {name}.parquet ({len(df)} lignes)")
    return df


def wavg(df, col, w="poids"):
    """Moyenne pondérée d'une colonne."""
    weights = df[w].fillna(1) if w in df.columns else pd.Series(1, index=df.index)
    vals = df[col].fillna(0)
    return float(np.average(vals, weights=weights))


# ══════════════════════════════════════════════════════════════════════════════
# KPI PARITÉ — DS1
# ══════════════════════════════════════════════════════════════════════════════

def kpi_parite(ds1):
    logger.info("── KPI Parité DS1 ──")

    ann = (ds1.groupby(["channel","media_type","year"])
           .agg(pct_female_mean=("pct_female","mean"),
                pct_female_min=("pct_female","min"),
                pct_female_max=("pct_female","max"),
                pct_female_std=("pct_female","std"),
                n_records=("pct_female","count"))
           .reset_index().round(2))
    ann["above_arcom"]  = (ann["pct_female_mean"] >= ARCOM_THRESHOLD).astype(int)
    ann["ecart_parite"] = (50 - ann["pct_female_mean"]).round(2)

    # Projection parité 50% par régression linéaire
    proj = []
    for ch, g in ann.groupby("channel"):
        g = g.sort_values("year")
        if len(g) >= 3:
            sl, inter, r, _, _ = sp.linregress(g["year"], g["pct_female_mean"])
            yr50 = int((50-inter)/sl) if sl > 0 else None
            proj.append({"channel":ch,"slope_annuel":round(sl,4),
                         "r2":round(r**2,3),"annee_parite_50":yr50})

    # Mensuel + rolling 12m
    w = ML_PARAMS["rolling_window_months"]
    mon = (ds1.groupby(["channel","media_type","year","month","year_month"])
           .agg(pct_female_mean=("pct_female","mean"))
           .reset_index().round(2))
    mon["rolling_12m"] = (mon.groupby("channel")["pct_female_mean"]
                          .transform(lambda x: x.rolling(w, min_periods=3).mean()).round(2))
    nat = ds1.groupby(["year","month","media_type"])["pct_female"].mean().reset_index()
    nat.columns = ["year","month","media_type","national_avg"]
    mon = mon.merge(nat, on=["year","month","media_type"], how="left")
    mon["ecart_national"] = (mon["pct_female_mean"] - mon["national_avg"]).round(2)
    mon["above_avg"]      = (mon["ecart_national"] > 0).astype(int)
    mon["media_type_enc"] = (mon["media_type"] == "radio").astype(int)

    save(ann, "kpi_parite_annuel")
    save(pd.DataFrame(proj), "kpi_parite_projection")
    save(mon, "kpi_parite_mensuel")

    # Features ML
    ml = mon.dropna(subset=["rolling_12m","ecart_national"])
    save(ml, "ml_features")


# ══════════════════════════════════════════════════════════════════════════════
# KPI CSA — DS2
# ══════════════════════════════════════════════════════════════════════════════

def kpi_csa(ds2):
    logger.info("── KPI CSA DS2 ──")
    metrics = [c for c in ["pct_female_presence","pct_female_speech",
                            "pct_female_experts","pct_female_journalists"] if c in ds2.columns]
    save(ds2[["channel","media_type","year"] + metrics].copy(), "kpi_csa")


# ══════════════════════════════════════════════════════════════════════════════
# KPI THÈMES — DS3
# ══════════════════════════════════════════════════════════════════════════════

def kpi_themes(ds3):
    logger.info("── KPI Thèmes DS3 ──")

    evo = (ds3.groupby(["channel","theme","year"])["pct_sujets_norm"]
           .mean().reset_index().round(2))
    save(evo, "kpi_themes_evolution")

    fd_evo = evo[evo["theme"] == "Faits divers"].copy()
    fd_pivot = fd_evo.pivot_table(index="year", columns="channel",
                                   values="pct_sujets_norm").reset_index()
    if "TF1" in fd_pivot.columns and "France 2" in fd_pivot.columns:
        fd_pivot["ecart_tf1_f2"] = (fd_pivot["TF1"] - fd_pivot["France 2"]).round(2)
    save(fd_pivot, "kpi_faits_divers")

    heatmap = (ds3.groupby(["channel","theme"])["pct_sujets_norm"]
               .mean().reset_index().round(2))
    save(heatmap, "kpi_heatmap_themes")


# ══════════════════════════════════════════════════════════════════════════════
# KPI SOURCES D'INFO — DS4
# ══════════════════════════════════════════════════════════════════════════════

def kpi_info(ds4):
    logger.info("── KPI Sources d'info DS4 (ARCOM) ──")
    info_cols = [c for c in ds4.columns if any(k in c for k in
                 ["source_info","confiance","rs_unique","interet"])]
    rows = []
    for ag, g in ds4.groupby("age_groupe"):
        row = {"age_groupe": ag}
        for c in info_cols:
            if c in g.columns:
                row[f"pct_{c}"] = round(wavg(g, c)*100, 1)
        rows.append(row)
    save(pd.DataFrame(rows), "kpi_francais_info")


# ══════════════════════════════════════════════════════════════════════════════
# KPI USAGES & ÉCRANS — DS5
# ══════════════════════════════════════════════════════════════════════════════

def kpi_usages(ds5):
    logger.info("── KPI Tendances audio-vidéo DS5 (ARCOM) ──")
    usage_cols = [c for c in ds5.columns if any(k in c for k in
                  ["tv_lin","svod","youtube","rs_video","radio","podcast",
                   "smart","smartphone"])]
    rows = []
    for ag, g in ds5.groupby("age_groupe"):
        row = {"age_groupe": ag}
        for c in usage_cols:
            if c in g.columns:
                row[f"pct_{c}"] = round(wavg(g, c)*100, 1)
        rows.append(row)
    save(pd.DataFrame(rows), "kpi_tendances_av")


# ══════════════════════════════════════════════════════════════════════════════
# CROISEMENTS INTER-DATASETS
# ══════════════════════════════════════════════════════════════════════════════

def croisements(ds1, ds2, ds3, ds4, ds5):
    logger.info("── Croisements inter-datasets ──")

    # DS1 × DS3 : parité ↔ faits divers par chaîne
    parite = (ds1[ds1["channel"].isin(CHANNELS_DS3)]
              .groupby(["channel","year"])["pct_female"].mean().reset_index())
    parite.columns = ["channel","year","pct_female_mean"]
    fd = (ds3[ds3["theme"]=="Faits divers"]
          .groupby(["channel","year"])["pct_sujets_norm"].mean().reset_index())
    fd.columns = ["channel","year","pct_faits_divers"]
    cr_ds1_ds3 = parite.merge(fd, on=["channel","year"], how="inner")

    corr_rows = []
    for ch, g in cr_ds1_ds3.groupby("channel"):
        if len(g) >= 5:
            r, p = sp.pearsonr(g["pct_female_mean"], g["pct_faits_divers"])
            corr_rows.append({"channel":ch,"pearson_r":round(r,3),"p_value":round(p,4),
                              "sens":"négatif" if r<-0.3 else "positif" if r>0.3 else "neutre"})
    save(cr_ds1_ds3, "croisement_ds1_ds3")
    save(pd.DataFrame(corr_rows), "corr_parite_faitsdiv")

    # DS1 × DS2 : INA automatique vs CSA officiel
    if "pct_female_speech" in ds2.columns:
        ina_yr = (ds1.groupby(["channel","year"])["pct_female"]
                  .mean().reset_index().rename(columns={"pct_female":"pct_ina"}))
        csa_yr = ds2[["channel","year","pct_female_speech"]].rename(
            columns={"pct_female_speech":"pct_csa"})
        cr = ina_yr.merge(csa_yr, on=["channel","year"], how="inner")
        cr["ecart_ina_csa"] = (cr["pct_ina"] - cr["pct_csa"]).round(2)
        save(cr, "croisement_ds1_ds2")

    # DS4 × DS5 : confiance médias / abandon TV / montée RS
    info_cols = [c for c in ds4.columns if "source_info_tv" in c or "confiance" in c]
    usage_cols = [c for c in ds5.columns if "tv_lin" in c or "rs_video" in c]
    if info_cols and usage_cols:
        rows = []
        for ag in ds4["age_groupe"].unique():
            g4 = ds4[ds4["age_groupe"]==ag]
            g5 = ds5[ds5["age_groupe"]==ag] if "age_groupe" in ds5.columns else ds5
            row = {"age_groupe": ag}
            for c in info_cols: row[f"info_{c}"] = round(wavg(g4, c)*100, 1)
            for c in usage_cols: row[f"av_{c}"] = round(wavg(g5, c)*100, 1)
            rows.append(row)
        save(pd.DataFrame(rows), "croisement_ds4_ds5")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("═" * 60)
    logger.info("ÉTAPE 3 — GOLD : KPIs + Croisements (5 datasets)")
    logger.info("═" * 60)

    client = get_client()
    ds1 = download_parquet(client, BUCKET_SILVER, SILVER["ds1"])
    ds2 = download_parquet(client, BUCKET_SILVER, SILVER["ds2"])
    ds3 = download_parquet(client, BUCKET_SILVER, SILVER["ds3"])
    ds4 = download_parquet(client, BUCKET_SILVER, SILVER["ds4"])
    ds5 = download_parquet(client, BUCKET_SILVER, SILVER["ds5"])

    kpi_parite(ds1)
    kpi_csa(ds2)
    kpi_themes(ds3)
    kpi_info(ds4)
    kpi_usages(ds5)
    croisements(ds1, ds2, ds3, ds4, ds5)

    logger.info("═" * 60)
    logger.info("✅ Features Gold complètes — → python ml/train_model.py")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
