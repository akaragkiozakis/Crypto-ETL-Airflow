from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

def fetch_price_to_dataframe():
    url = "https://api.coingecko.com/api/v3/simple/price"
    currencies = ["bitcoin", "ethereum", "solana", "ripple", "litecoin"]
    params = {
        "ids": ",".join(currencies),
        "vs_currencies": "usd"
    }

    response = requests.get(url, params=params)
    data = response.json()
    timestamp = datetime.utcnow()

    # creating dataframes for all currencies
    rows = []
    for coin in currencies:
        price = data.get(coin, {}).get("usd", None)
        if price is not None:
            rows.append({
                "time": timestamp,
                "currency": coin,
                "price_usd": price
            })

    df = pd.DataFrame(rows)

    print("----- DataFrame -----")
    print(df)

    # save to csv file
    file_path = '/opt/airflow/logs/crypto_prices.csv'
    df.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))

# Default settings
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
with DAG(
    dag_id='multi_crypto_price_to_dataframe',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='fetch_and_log_crypto_prices',
        python_callable=fetch_price_to_dataframe
    )
