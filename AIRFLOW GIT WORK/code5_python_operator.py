from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from datetime import datetime, timedelta

#this package will be required for the task
from airflow.operators.python import PythonOperator

from pandas import json_normalize

default_args = {
    'owner': 'ss',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
def _store_user():
    hook = PostgresHook(
        postgres_conn_id = 'postgres'
    )

    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


def _process_user(ti): #ti= task instance- to pull the data that has been loaded buy the task xcom_user
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname' : user['name']['first'],
        'lastname' : user['name']['last'],
        'country' : user['location']['country'],
        'username' : user['login']['username'],
        'password' : user['login']['password'],
        'email' : user['email']
    })

    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)


with DAG(
    dag_id='code5_python_operator',
    default_args=default_args,
    description='This DAG is created to process details of the user using Python Operator',
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
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET', # requesting to get the data 
        response_filter= lambda response: json.loads(response.text), #in order to extract the data and transform the data into json format
        log_response=True #to log the reponse and will able to see the response on logs
    )
        
    process_user = PythonOperator(
        task_id='process_user',
        python_callable= _process_user
    )

    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable = _store_user
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user