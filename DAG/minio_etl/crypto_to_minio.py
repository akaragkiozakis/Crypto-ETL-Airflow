from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='crypto_to_minio',
    default_args=default_args,
    description='Fetch crypto prices and write Parquet files to MinIO (S3-compatible)',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def crypto_to_minio():
    @task()
    def get_params():
        coins = Variable.get('coins', default_var='["bitcoin","ethereum"]', deserialize_json=True)
        vs_currency = Variable.get('vs_currency', default_var='usd')
        s3_bucket = Variable.get('s3_bucket', default_var='crypto')
        s3_base_path = Variable.get('s3_base_path', default_var='raw/crypto_prices')
        return {
            'coins': coins,
            'vs_currency': vs_currency,
            's3_bucket': s3_bucket,
            's3_base_path': s3_base_path,
        }

    @task()
    def fetch_market_data(cfg):
        coin_ids = ','.join(cfg['coins'])
        url = f'https://api.coingecko.com/api/v3/coins/markets?vs_currency={cfg["vs_currency"]}&ids={coin_ids}'
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        df['timestamp'] = datetime.utcnow().strftime('%d/%m/%Y %H:%M:%S')
        temp_path = '/tmp/crypto_data.parquet'
        df.to_parquet(temp_path)
        return temp_path

    @task()
    def write_parquet_to_minio(cfg, temp_path):
        df = pd.read_parquet(temp_path)
        if df.empty:
            print("No data to write to MinIO.")
            return
        s3_bucket = cfg['s3_bucket']
        s3_base_path = cfg['s3_base_path']
        minio_access_key = Variable.get('minio_access_key', default_var='minioadmin')
        minio_secret_key = Variable.get('minio_secret_key', default_var='minioadmin123')
        minio_endpoint = Variable.get('minio_endpoint', default_var='http://minio:9000')
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
        fs = s3fs.S3FileSystem(
            key=minio_access_key,
            secret=minio_secret_key,
            client_kwargs={'endpoint_url': minio_endpoint}
        )
        for coin in df['id'].unique():
            df_coin = df[df['id'] == coin]
            path = f"{s3_bucket}/{s3_base_path}/date={date_str}/coin={coin}/data.parquet"
            table = pa.Table.from_pandas(df_coin)
            with fs.open(path, 'wb') as f:
                pq.write_table(table, f)
        print(f"Wrote data for coins {list(df['id'].unique())} to MinIO.")

    cfg = get_params()
    temp_path = fetch_market_data(cfg)
    write_parquet_to_minio(cfg, temp_path)

dag = crypto_to_minio()
