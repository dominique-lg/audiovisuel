import pandas as pd
from minio import Minio
import io
import json

# Configuration client
client = Minio("localhost:9000", access_key="admin", secret_key="password", secure=False)
bucket_name = "datalake"

# Lister les objets dans la couche Bronze
objects = client.list_objects(bucket_name, prefix="bronze/", recursive=True)
all_frames = []

for obj in objects:
    response = client.get_object(bucket_name, obj.object_name)
    content = response.read().decode('utf-8')
    
    # Correction : Charger le JSON et le transformer immédiatement en DataFrame
    data_list = json.loads(content)
    all_frames.append(pd.DataFrame(data_list))

# On concatène tous les fichiers lus en un seul DataFrame propre
if all_frames:
    df_silver = pd.concat(all_frames, ignore_index=True)
else:
    print("Aucune donnée trouvée dans la couche Bronze.")
    exit()

print(df_silver.info()) 

# --- Nettoyage ---
# Maintenant que c'est un DataFrame plat, drop_duplicates fonctionnera
df_silver.drop_duplicates(inplace=True)

# Vérifie bien les noms de colonnes issus de ton ingestion Bronze
# (Normalement : date, channel, theme, nb_subjects, duration_seconds)
df_silver.dropna(subset=['theme', 'channel'], inplace=True)

# --- Typage ---
df_silver['date'] = pd.to_datetime(df_silver['date'])
df_silver['nb_subjects'] = pd.to_numeric(df_silver['nb_subjects'], errors='coerce').fillna(0).astype(int)

# --- Structuration pour l’analyse ---
df_silver['year'] = df_silver['date'].dt.year
df_silver['month'] = df_silver['date'].dt.month

# --- Export vers Parquet ---
parquet_buffer = io.BytesIO()
df_silver.to_parquet(parquet_buffer, index=False)
parquet_buffer.seek(0)

# Upload vers Silver
silver_path = f"silver/ina_audiovisuel_cleaned.parquet"
client.put_object(
    bucket_name,
    silver_path,
    parquet_buffer,
    length=parquet_buffer.getbuffer().nbytes,
    content_type='application/octet-stream'
)

print(f"✅ Données Silver écrites avec succès : {silver_path}")

# --- Traitement du dataset Représentation des Femmes (Silver) ---

# 1. Lister les objets spécifiques à ce dataset dans la couche Bronze
objects_women = client.list_objects(bucket_name, prefix="bronze/date=", recursive=True)
all_frames_women = []

for obj in objects_women:
    if "representation_femmes" in obj.object_name:
        response = client.get_object(bucket_name, obj.object_name)
        data_list = json.loads(response.read().decode('utf-8'))
        all_frames_women.append(pd.DataFrame(data_list))

if all_frames_women:
    df_women_silver = pd.concat(all_frames_women, ignore_index=True)
    
    # --- Nettoyage et Typage ---
    df_women_silver.drop_duplicates(inplace=True)
    
    # Conversion des taux en numériques au cas où 
    df_women_silver['women_expression_rate'] = pd.to_numeric(df_women_silver['women_expression_rate'], errors='coerce')
    df_women_silver['year'] = df_women_silver['year'].astype(int)
    
    # --- Export vers Parquet ---
    parquet_buffer_women = io.BytesIO()
    df_women_silver.to_parquet(parquet_buffer_women, index=False)
    parquet_buffer_women.seek(0)

    # Upload vers Silver
    silver_path_women = "silver/representation_femmes_cleaned.parquet"
    client.put_object(
        bucket_name,
        silver_path_women,
        parquet_buffer_women,
        length=parquet_buffer_women.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    print(f"✅ Données Femmes Silver écrites : {silver_path_women}")
else:
    print("⚠️ Aucun fichier de représentation des femmes trouvé en Bronze.")