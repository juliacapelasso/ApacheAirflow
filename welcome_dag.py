from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
    'welcome_dag',
    start_date=days_ago(1),
    schedule_interval='@daily',
) as dag:

    def welcome():
        print("Welcome to Apache Airflow!")
   
    task_1 = PythonOperator(
        task_id = 'welcome',
        python_callable=welcome
    )
