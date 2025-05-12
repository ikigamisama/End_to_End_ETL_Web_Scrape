
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG

import os

load_dotenv(dotenv_path="/opt/airflow/.env")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_DRIVER = os.getenv("DB_DRIVER")


default_args = {
    "owner": "Ikigami",
    "start_date": datetime(2025, 5, 3),
}


def check_and_initialize():
    from ETL.libs.utils import get_db_conn, get_sql_from_file, execute_query
    engine = get_db_conn(
        DB_DRIVER,
        DB_USER,
        DB_PASSWORD,
        DB_HOST,
        DB_PORT,
        DB_NAME,
    )

    sql = get_sql_from_file('init.sql')
    execute_query(engine, sql)


with DAG(
    dag_id="init_database_schema",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Initializes the smartphone ETL database schema if not present",
) as dag:

    initialize_db = PythonOperator(
        task_id="check_and_initialize_db",
        python_callable=check_and_initialize,
    )
