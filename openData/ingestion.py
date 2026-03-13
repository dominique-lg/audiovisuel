import pandas as pd
from minio import Minio
from datetime import datetime
import io
import os

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

bucket_name = "datalake"
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

def ingest_to_bronze(file_path, dataset_name):
    df = pd.read_csv(file_path)

    today = datetime.now().strftime('%Y-%m-%d')
    data_json = df.to_json(orient='records')
    data_bytes = data_json.encode('utf-8')

    object_name = f"bronze/date={today}/{dataset_name}.json"

    client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type='application/json'
    )
    print(f"✅ Ingestion réussie : {object_name}")

base_dir = os.path.dirname(os.path.abspath(__file__))

ingest_to_bronze(os.path.join(base_dir, 'ina_daily_data_cleaned.csv'), 'ina_audiovisuel')
ingest_to_bronze(os.path.join(base_dir, 'women_representation_years_cleaned.csv'), 'representation_femmes')