"""
With this pipeline we insert data into crypto_incremental table,
as we want to take the maximum timestamp from this table and insert the data to raw table.
So it's obvious that crypto_incremental table has to has data.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'insert_incremental_data_once',
    default_args=default_args,
    description='One-off DAG to insert a small batch into crypto_incremental',
    schedule_interval=None,
    catchup=False,
)


def seed_incremental_data() -> None:
    database_connection = mysql.connector.connect(
        host='mysql',
        port=3306,
        user='root',
        password='my-secret-pw',
        database='crypto_db',
    )
    cursor = database_connection.cursor()

    batch_timestamp = datetime.utcnow().strftime('%d/%m/%Y %H:%M:%S')

    rows_to_insert = [
        (batch_timestamp, 'bitcoin', 64000.0),
        (batch_timestamp, 'ethereum', 3100.0),
    ]

    insert_statement = (
        """
        INSERT IGNORE INTO crypto_incremental (timestamp, coin_id, price_usd)
        VALUES (%s, %s, %s)
        """
    )

    for row in rows_to_insert:
        cursor.execute(insert_statement, row)

    database_connection.commit()
    cursor.close()
    database_connection.close()


seed_task = PythonOperator(
    task_id='insert_incremental_data',
    python_callable=seed_incremental_data,
    dag=dag,
)
