"""airflow/dags/ina_pipeline_dag.py — DAG mensuel Bronze → Silver → Gold (5 datasets)"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ina_pipeline",
    description="Pipeline INA Audiovisuel — 5 datasets — Bronze → Silver → Gold",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner":"maelle.fotso","retries":2,
                  "retry_delay":timedelta(minutes=5),"email_on_failure":False},
    tags=["ina","audiovisuel","arcom","open-data"],
) as dag:

    start   = BashOperator(task_id="start",  bash_command="echo '=== Pipeline INA démarré ==='")
    bronze  = BashOperator(task_id="bronze", bash_command="cd /opt/airflow && python ingestion/download_all.py")
    silver  = BashOperator(task_id="silver", bash_command="cd /opt/airflow && python processing/clean_all.py")
    quality = BashOperator(task_id="quality_check", bash_command="""
python -c "
import sys; sys.path.insert(0,'.')
from config.minio_utils import get_client, download_parquet
from config.config import BUCKET_SILVER, SILVER
c = get_client()
for k,p in SILVER.items():
    df = download_parquet(c, BUCKET_SILVER, p)
    assert len(df) > 0, f'{k} est vide !'
    print(f'✅ {k}: {len(df)} lignes')
print('Quality check OK')
"
    """)
    feat    = BashOperator(task_id="features", bash_command="cd /opt/airflow && python ml/features.py")
    train   = BashOperator(task_id="ml",       bash_command="cd /opt/airflow && python ml/train_model.py")
    gold    = BashOperator(task_id="gold",     bash_command="cd /opt/airflow && python ml/upload_to_gold.py")
    end     = BashOperator(task_id="end",      bash_command="echo '=== Pipeline terminé — exports Power BI dans gold/powerbi_exports/ ==='")

    start >> bronze >> silver >> quality >> feat >> train >> gold >> end
