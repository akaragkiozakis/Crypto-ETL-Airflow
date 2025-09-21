"""
With this pipeline we create 2 tables to mysql, named crypto_raw and crypto_incremental
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'setup_crypto_tables',
    default_args=default_args,
    description='One-off DAG to create crypto tables',
    schedule_interval=None,
    catchup=False,
)


def create_tables():
    mydb = mysql.connector.connect(
        host='mysql',
        port=3306,
        user='root',
        password='my-secret-pw',
        database='crypto_db',
    )

    mycursor = mydb.cursor()

    mycursor.execute("SHOW TABLES LIKE 'crypto_raw'")
    raw_exists = mycursor.fetchone()

    mycursor.execute("SHOW TABLES LIKE 'crypto_incremental'")
    incremental_exists = mycursor.fetchone()

    if not raw_exists:
        mycursor.execute(
            """
            CREATE TABLE crypto_raw (
                timestamp VARCHAR(25),
                coin_id VARCHAR(50),
                price_usd FLOAT,
                PRIMARY KEY (timestamp, coin_id)
            )
            """
        )
        print('Created crypto_raw table')
    else:
        print('crypto_raw table already exists')

    if not incremental_exists:
        mycursor.execute(
            """
            CREATE TABLE crypto_incremental (
                timestamp VARCHAR(25),
                coin_id VARCHAR(50),
                price_usd FLOAT,
                PRIMARY KEY (timestamp, coin_id)
            )
            """
        )
        print('Created crypto_incremental table')
    else:
        print('crypto_incremental table already exists')

    if raw_exists and incremental_exists:
        print('All tables already exist, skipping creation')
    else:
        print('Tables creation completed')

    mydb.commit()
    mycursor.close()
    mydb.close()


create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)
