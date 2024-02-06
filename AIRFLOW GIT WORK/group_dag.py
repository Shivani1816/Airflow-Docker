from airflow import DAG
from airflow.operators.bash import BashOperator

#importing the subdag so that it can call subdags
from airflow.operators.subdag import SubDagOperator

#importing subdag_downloads file from subdags folder
from subdags.subdag_downloads import subdag_downloads

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
    downloads = SubDagOperator(
        task_id= 'downloads',
        subdag= subdag_downloads(dag.dag_id, 'downloads', args) #first agrument is Parent Dag ID, second is child Dag ID
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'        
    )
    transform_a = BashOperator(
        task_id='transform_a',
        bash_command='sleep 10'        
    )
    transform_b = BashOperator(
        task_id='transform_b',
        bash_command='sleep 10'        
    )
    transform_c = BashOperator(
        task_id='transform_c',
        bash_command='sleep 10'        
    )  
    #dependencies 
    downloads >> check_files >> [transform_a, transform_b, transform_c]
    
#---------------------------------------------------------------------------------------------------------------
    # download_a= BashOperator(
    #     task_id='download_a',
    #     bash_command='sleep 10'
    # )
    # download_b= BashOperator(
    #     task_id='download_b',
    #     bash_command='sleep 10'
    # )
    # download_c= BashOperator(
    #     task_id='download_c',
    #     bash_command='sleep 10'
    # )
    #dependencies
    #  [download_a, download_b, download_c] >> check_files >> [transform_a, transform_b, transform_c]