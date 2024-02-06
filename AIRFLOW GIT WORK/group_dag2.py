                                    #TASK GROUPS - easier and less code than subdags

from airflow import DAG
from airflow.operators.bash import BashOperator

#importimg files from group folder
from groups.group_downloads import download_tasks
from groups.group_tranforms import transform_tasks

from datetime import datetime

with DAG(
    dag_id='group_dag',
    start_date=datetime(2024, 1, 29),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    args = {
        'start_date': dag.start_date,
        'schedule_interval': dag.schedule_interval,
        'catchup': dag.catchup
    }
    downloads = download_tasks()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'        
    )

    transforms = transform_tasks()
 
    #dependencies 
    downloads >> check_files >> transforms