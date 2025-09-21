"""
With this pipeline we insert data from an api to crypto incremental table.
We take the max timestamp from incremental table and insert the data to the raw table.
With this pipeline, the raw table will only have the most recent data.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'crypto_etl_pipeline',
    default_args=default_args,
    description= 'Pipeline for ingesting crypto data',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

 

def insert_data_to_crypto_incremental():
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin,ethereum'
    response = requests.get(url)
    data = response.json()

    mydb = mysql.connector.connect(
        host='mysql',
        port=3306,
        user='root',
        password='my-secret-pw',
        database='crypto_db'
    )

    mycursor = mydb.cursor()

    insert_query = """
        INSERT INTO crypto_incremental (timestamp, coin_id, price_usd)
        VALUES (%s, %s, %s)
    """

    current_time = datetime.utcnow()
    timestamp_str = current_time.strftime('%d/%m/%Y %H:%M:%S')

    for coin in data:
        coin_id = coin['id']
        price_usd = coin['current_price']
        mycursor.execute(insert_query, (timestamp_str, coin_id, price_usd))

    mydb.commit()
    mycursor.close()
    mydb.close()
    print('Inserted data to crypto_incremental from API')

def update_raw_table():
    mydb = mysql.connector.connect(
        host='mysql',
        port=3306,
        user='root',
        password='my-secret-pw',
        database='crypto_db'
    )
    mycursor = mydb.cursor()


    mycursor.execute("SELECT MAX(timestamp) FROM crypto_incremental")
    max_inc_timestamp = mycursor.fetchone()[0]

    if max_inc_timestamp is None:
        print('No data found in crypto_incremental. Nothing to copy to crypto_raw.')
        mycursor.close()
        mydb.close()
        return


    mycursor.execute(
        """
        SELECT timestamp, coin_id, price_usd
        FROM crypto_incremental
        WHERE timestamp = %s
        """,
        (max_inc_timestamp,)
    )
    latest_rows = mycursor.fetchall()

    insert_query = """
        INSERT IGNORE INTO crypto_raw (timestamp, coin_id, price_usd)
        VALUES (%s, %s, %s)
    """
    for row in latest_rows:
        mycursor.execute(insert_query, row)

    mydb.commit()
    mycursor.close()
    mydb.close()
    print(f'Inserted {len(latest_rows)} latest rows (timestamp={max_inc_timestamp}) to crypto_raw')
    return len(latest_rows)


insert_task = PythonOperator(
    task_id='insert_to_incremental',
    python_callable=insert_data_to_crypto_incremental,
    dag=dag
)

update_task = PythonOperator(
    task_id='update_raw',
    python_callable=update_raw_table,
    dag=dag
)

insert_task >> update_task
