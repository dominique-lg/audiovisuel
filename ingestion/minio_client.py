from minio import Minio
import pandas as pd
import io

# CORRECTION : On enlève l'espace avant le 10
IP_SERVEUR = "10.135.116.139" 

client = Minio(
    f"{IP_SERVEUR}:9000",
    access_key="admin",
    secret_key="password",
    secure=False,
    http_client=__import__('urllib3').PoolManager(timeout=10)  # 10 second timeout

)

def load_silver_data(file_name):
    try:
        # On vérifie que le bucket et le fichier existent
        response = client.get_object("datalake", f"silver/{file_name}")
        # Lecture du format Parquet optimisé pour la couche Silver
        return pd.read_parquet(io.BytesIO(response.read()))
    except Exception as e:
        print(f" Erreur lors du chargement de {file_name}: {e}")
        return None

# Chargement des deux datasets pour l'étape Gold (Analyse & ML)
df_ina = load_silver_data("ina_audiovisuel_cleaned.parquet")
df_femmes = load_silver_data("representation_femmes_cleaned.parquet")

if df_ina is not None and df_femmes is not None:
    print(" Données Silver (INA & Parité) prêtes pour l'analyse Gold !")