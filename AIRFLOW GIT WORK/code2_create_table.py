from airflow import DAG

#this package will be required for the task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime


with DAG(
    dag_id='code2_table_creation',
    start_date=datetime(2024, 1, 19),
    schedule_interval='@daily',
    catchup=False
) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL, 
            email TEXT NOT NULL
            );
        '''
    )