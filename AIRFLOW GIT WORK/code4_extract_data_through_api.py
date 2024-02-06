from airflow import DAG
from datetime import datetime, timedelta

#this package will be required for the task
from airflow.providers.http.operators.http import SimpleHttpOperator

import json

default_args = {
    'owner': 'ss',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='code4_extract_data',
    default_args=default_args,
    description='This functionality is to extract data through API using JSON',
    start_date= datetime(2024, 1, 19),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET', # requesting to get the data 
        response_filter= lambda response: json.loads(response.text), #in order to extract the data and transform the data into json format
        log_response=True #to log the reponse and will able to see the response on logs
    )
