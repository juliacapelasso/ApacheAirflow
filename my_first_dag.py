#Import the libraries and operators that will be used.
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

#Create the DAG and specify the tasks.
with DAG(
  'my_first_dag',
  start_date=days_ago(1),
  schedule_interval='@daily',
) as dag:

   task_1 = EmptyOperator(task_id='task_1')
   task_2 = EmptyOperator(task_id='task_2')
   task_3 = EmptyOperator(task_id='task_3')
   task_4 = BashOperator(
        task_id = 'create_folder',
        #Create the Linux folder where the DAG will be stored.
        bash_command = 'mkdir -p "/home/juliacapelasso/Documents/airflowalura/pasta"'
      )
#Create the order that they'll be executed .
   task_1 >> [task_2, task_3] 
   task_3 >> task_4