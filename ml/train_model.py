"""ml/train_model.py — KNN + Random Forest (profil parité chaînes — DS1)"""
import sys, json, logging
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import ML_PARAMS

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("train_model")

GOLD_DIR = Path(__file__).parent.parent / "data" / "gold"
FEATURES = ["pct_female_mean","rolling_12m","ecart_national","month","year","media_type_enc"]
LABEL    = "above_avg"


def main():
    logger.info("══ ML : KNN + Random Forest ══")
    p = GOLD_DIR / "ml_features.parquet"
    if not p.exists():
        logger.error("Lancer d'abord : python ml/features.py"); return

    df = pd.read_parquet(p).dropna(subset=FEATURES+[LABEL])
    X, y = df[FEATURES].values, df[LABEL].values
    logger.info(f"  {X.shape[0]} exemples · classes : {dict(zip(*np.unique(y,return_counts=True)))}")

    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=ML_PARAMS["test_size"],
                                            random_state=ML_PARAMS["random_state"], stratify=y)
    sc = StandardScaler()
    Xtr_s, Xte_s = sc.fit_transform(Xtr), sc.transform(Xte)

    results = {}

    knn = KNeighborsClassifier(n_neighbors=ML_PARAMS["knn_neighbors"])
    knn.fit(Xtr_s, ytr)
    ypk = knn.predict(Xte_s)
    cvk = cross_val_score(knn, Xtr_s, ytr, cv=5, scoring="f1_macro")
    results["knn"] = {"accuracy":round(accuracy_score(yte,ypk),4),
                      "f1_macro":round(f1_score(yte,ypk,average="macro"),4),
                      "cv_f1_mean":round(cvk.mean(),4), "cv_f1_std":round(cvk.std(),4),
                      "confusion_matrix":confusion_matrix(yte,ypk).tolist()}
    logger.info(f"  KNN  Acc={results['knn']['accuracy']:.3f} F1={results['knn']['f1_macro']:.3f}")

    rf = RandomForestClassifier(n_estimators=ML_PARAMS["rf_n_estimators"],
                                 random_state=ML_PARAMS["random_state"], n_jobs=-1)
    rf.fit(Xtr, ytr)
    ypr = rf.predict(Xte)
    cvr = cross_val_score(rf, Xtr, ytr, cv=5, scoring="f1_macro")
    fi  = dict(zip(FEATURES, rf.feature_importances_.round(4).tolist()))
    results["random_forest"] = {"accuracy":round(accuracy_score(yte,ypr),4),
                                 "f1_macro":round(f1_score(yte,ypr,average="macro"),4),
                                 "cv_f1_mean":round(cvr.mean(),4), "cv_f1_std":round(cvr.std(),4),
                                 "confusion_matrix":confusion_matrix(yte,ypr).tolist(),
                                 "feature_importances":fi}
    logger.info(f"  RF   Acc={results['random_forest']['accuracy']:.3f} F1={results['random_forest']['f1_macro']:.3f}")
    logger.info(f"  FI : {fi}")

    best = max(["knn","random_forest"], key=lambda m: results[m]["f1_macro"])
    results.update({"best_model":best,"features":FEATURES,
                    "n_samples":int(X.shape[0]),"run_date":datetime.now().isoformat()})

    with open(GOLD_DIR/"model_metrics.json","w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    pd.DataFrame(Xte, columns=FEATURES).assign(
        y_true=yte, y_pred_rf=ypr, y_pred_knn=ypk
    ).to_parquet(GOLD_DIR/"ml_predictions.parquet", index=False)

    logger.info(f"  ✅ Meilleur modèle : {best.upper()}")
    logger.info("  → python ml/upload_to_gold.py")


if __name__ == "__main__":
    main()
