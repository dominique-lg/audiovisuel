"""config/minio_utils.py — Helpers MinIO partagés"""
import io, json, logging
import pandas as pd
from minio import Minio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE

logger = logging.getLogger(__name__)

def get_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)

def ensure_bucket(client, bucket):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info(f"Bucket créé : {bucket}")

def upload_csv(client, bucket, path, df):
    ensure_bucket(client, bucket)
    data = df.to_csv(index=False).encode("utf-8")
    client.put_object(bucket, path, io.BytesIO(data), len(data), content_type="text/csv")
    logger.info(f"  ✅ CSV → {bucket}/{path} ({len(df)} lignes)")

def upload_parquet(client, bucket, path, df):
    ensure_bucket(client, bucket)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0); raw = buf.read()
    client.put_object(bucket, path, io.BytesIO(raw), len(raw))
    logger.info(f"  ✅ Parquet → {bucket}/{path} ({len(df)} lignes)")

def upload_json(client, bucket, path, data):
    ensure_bucket(client, bucket)
    raw = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(bucket, path, io.BytesIO(raw), len(raw), content_type="application/json")
    logger.info(f"  ✅ JSON → {bucket}/{path}")

def download_csv(client, bucket, path, sep=","):
    resp = client.get_object(bucket, path)
    df = pd.read_csv(io.BytesIO(resp.read()), sep=sep)
    logger.info(f"  ⬇️  {bucket}/{path} ({len(df)} lignes)")
    return df

def download_parquet(client, bucket, path):
    resp = client.get_object(bucket, path)
    df = pd.read_parquet(io.BytesIO(resp.read()))
    logger.info(f"  ⬇️  {bucket}/{path} ({len(df)} lignes)")
    return df
