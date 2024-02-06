from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define default_args dictionary to specify default parameters for the DAG
default_args = {
    'owner': 'shivani',
    'start_date': datetime(2024, 1, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_output(ti):
    result = ti.xcom_pull(task_ids='gp_function_task')['result']
    print(f"The sum result is: {result}")


# Defining the DAG
with DAG(
    dag_id='greenplum_function_dag',
    default_args=default_args,
    schedule_interval='@daily',  
) as dag:

    #Defining the SQL query to call the Greenplum function
    sql_query = """
        SELECT sum(2,3) AS result;
    """

    #creating a task to call gp function
    gp_function_task = PostgresOperator(
        task_id='gp_function_task',
        sql=sql_query,
        postgres_conn_id='postgres',  
        autocommit=True
    )

    #printing the result value
    print_result = PythonOperator(
        task_id= 'print_result',
        python_callable= print_output,

    )

    #Setting task dependencies
    print_result.set_upstream(gp_function_task)


'''

----------------------SUPPOSE WE HAVE GP FUNCTION AS: Create a function that calculates the SUM of a number:-------

CREATE OR REPLACE FUNCTION sum(num1 INTEGER, num2 INTEGER)
RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    result := num1 + num2;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

----------------------------------Function End------------------------------------

-- Call the function in GP
SELECT sum(2,3) AS result;

'''