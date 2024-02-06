from airflow import DAG

#this package will be required for the task


from datetime import datetime,timedelta

default_args = {
    'owner': 'ss',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='code3_http_sensor',
    default_args=default_args,
    description='This is the sensor to check if the API is available or not',
    start_date= datetime(2024, 1, 19),
    schedule_interval='@daily',
    catchup=False
) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )