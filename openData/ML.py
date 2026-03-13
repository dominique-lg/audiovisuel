from minio import Minio
import pandas as pd
import io
import json

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler, LabelEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier

# =========================
# 1. Connexion MinIO
# =========================
client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

bucket_name = "datalake"


# =========================
# 2. Fonction de lecture parquet depuis MinIO
# =========================
def load_parquet_from_minio(object_name):
    response = client.get_object(bucket_name, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_parquet(io.BytesIO(data))


# =========================
# 3. Fonction d'écriture fichier texte/json/csv dans MinIO
# =========================
def upload_bytes_to_minio(object_name, data_bytes, content_type="application/octet-stream"):
    client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type=content_type
    )


# =========================
# 4. Chargement des données Silver
# =========================
df = load_parquet_from_minio("silver/ina_audiovisuel_cleaned.parquet")

print("Shape initiale :", df.shape)
print("Colonnes :", df.columns.tolist())

# On garde uniquement les colonnes utiles au modèle INA
expected_cols = ["date", "channel", "theme", "nb_subjects", "duration_seconds"]
df = df[[col for col in expected_cols if col in df.columns]].copy()

print("Shape après sélection colonnes :", df.shape)

# =========================
# 5. Nettoyage
# =========================
df.dropna(subset=["channel", "theme"], inplace=True)

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["nb_subjects"] = pd.to_numeric(df["nb_subjects"], errors="coerce")
df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce")

df.dropna(subset=["date", "nb_subjects", "duration_seconds"], inplace=True)

# Création de variables temporelles
df["year"] = df["date"].dt.year
df["month"] = df["date"].dt.month

# Nettoyage simple des chaînes texte
df["theme"] = df["theme"].astype(str).str.strip()
df["channel"] = df["channel"].astype(str).str.strip()

# Optionnel : enlever classes trop rares
channel_counts = df["channel"].value_counts()
valid_channels = channel_counts[channel_counts >= 50].index
df = df[df["channel"].isin(valid_channels)].copy()

print("Shape finale pour ML :", df.shape)
print("Répartition des chaînes :")
print(df["channel"].value_counts())

# =========================
# 6. Variables X / y
# =========================
X = df[["theme", "nb_subjects", "duration_seconds", "year", "month"]]
y = df["channel"]

label_encoder = LabelEncoder()
y_encoded = label_encoder.fit_transform(y)

# =========================
# 7. Train / Test split
# =========================
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y_encoded,
    test_size=0.2,
    random_state=42,
    stratify=y_encoded
)

# =========================
# 8. Préprocessing
# =========================
numeric_features = ["nb_subjects", "duration_seconds", "year", "month"]
categorical_features = ["theme"]

numeric_transformer = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features)
    ]
)

# =========================
# 9. Modèles
# =========================
models = {
    "KNN": KNeighborsClassifier(n_neighbors=7),
    "RandomForest": RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        class_weight="balanced"
    )
}

results = {}
best_model_name = None
best_accuracy = -1
best_pipeline = None
best_predictions = None

for model_name, model in models.items():
    pipeline = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", model)
    ])

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    report = classification_report(
        y_test,
        y_pred,
        target_names=label_encoder.classes_,
        output_dict=True
    )
    cm = confusion_matrix(y_test, y_pred)

    results[model_name] = {
        "accuracy": acc,
        "classification_report": report,
        "confusion_matrix": cm.tolist()
    }

    print(f"\n===== {model_name} =====")
    print("Accuracy :", acc)
    print(classification_report(y_test, y_pred, target_names=label_encoder.classes_))

    if acc > best_accuracy:
        best_accuracy = acc
        best_model_name = model_name
        best_pipeline = pipeline
        best_predictions = y_pred

# =========================
# 10. Sauvegarde des métriques en Gold
# =========================
metrics_json = json.dumps({
    "best_model": best_model_name,
    "best_accuracy": best_accuracy,
    "all_results": results
}, indent=2, ensure_ascii=False)

upload_bytes_to_minio(
    "gold/channel_classification_metrics.json",
    metrics_json.encode("utf-8"),
    content_type="application/json"
)

# =========================
# 11. Sauvegarde des prédictions en Gold
# =========================
pred_df = X_test.copy()
pred_df["true_channel"] = label_encoder.inverse_transform(y_test)
pred_df["predicted_channel"] = label_encoder.inverse_transform(best_predictions)

csv_buffer = io.StringIO()
pred_df.to_csv(csv_buffer, index=False)

upload_bytes_to_minio(
    "gold/channel_predictions.csv",
    csv_buffer.getvalue().encode("utf-8"),
    content_type="text/csv"
)

print("\n✅ Modèle entraîné avec succès")
print(f"✅ Meilleur modèle : {best_model_name}")
print(f"✅ Accuracy : {best_accuracy:.4f}")
print("✅ Résultats sauvegardés dans gold/")