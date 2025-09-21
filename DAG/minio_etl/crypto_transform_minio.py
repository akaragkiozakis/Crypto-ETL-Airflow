from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
import pyarrow.parquet as pq
import s3fs



default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='crypto_tranform_minio',
    default_args=default_args,
    description= 'Read Parquet data from MiniO',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def crypto_transform_minio():
    @task()
    def read_parquet_from_minio():
        

        s3_bucket = Variable.get('s3_bucket', default_var='crypto')
        s3_base_path = Variable.get('s3_base_path', default_var='raw/crypto_prices')
        minio_access_key = Variable.get('minio_access_key', default_var='minioadmin')
        minio_secret_key = Variable.get('minio_secret_key', default_var='minioadmin123')
        minio_endpoint = Variable.get('minio_endpoint', default_var='http://minio:9000')

        date_str = datetime.utcnow().strftime('%Y-%m-%d')
        coins = Variable.get('coins', default_var='["bitcoin","ethereum"]', deserialize_json=True)

        dfs = []
        fs = s3fs.S3FileSystem(
            key=minio_access_key,
            secret=minio_secret_key,
            client_kwargs={'endpoint_url': minio_endpoint}
        )
        for coin in coins:
            path = f"{s3_bucket}/{s3_base_path}/date={date_str}/coin={coin}/data.parquet"
            try:
                with fs.open(path, 'rb') as f:
                    table = pq.read_table(f)
                    df = table.to_pandas()
                    dfs.append(df)
            except Exception as e:
                print(f"Could not read {coin} data: {e}")

        if dfs:
            df_all = pd.concat(dfs, ignore_index=True)
            return df_all.to_dict()
        else:
            return {}

    @task()
    def calculate_moving_average(data):
        import pandas as pd
        if not data:
            print("No data to transform.")
            return

        df = pd.DataFrame(data)
        
        if 'price_usd' in df.columns:
            df['ma_7'] = df.groupby('coin_id')['price_usd'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
            print(df[['coin_id', 'timestamp', 'price_usd', 'ma_7']])
        else:
            print("price_usd column missing.")

    raw_data = read_parquet_from_minio()
    calculate_moving_average(raw_data)

dag = crypto_transform_minio()
