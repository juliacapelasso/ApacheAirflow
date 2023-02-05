from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#Import a date X days after Y date
from airflow.macros import  ds_add
#this library allows to define a specific date
import pendulum

#import the data from the API (extract_data_API.py)
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

with DAG(
    "climate_data",
    #First Monday of this current month (feb)
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    #execute every monday
    #0 minutes 0 hours * day * month * day/days of the week (1 = sunday), this is called Cron Expressions
    #you can insert * * because the program will work the whole year
    schedule_interval='0 0 * * 1',    
) as dag:
#Create folder to store information 
        task_1 = BashOperator(
            task_id = 'create_folder',
            #Create the Linux folder where the DAG will be stored.
            bash_command = 'mkdir -p "/home/juliacapelasso/Documents/airflowalura/week={{data_interval_end.strftime("%Y-%m-%d")}}"'
            #format to exib only the year day and month
        )

        #create a function that'll be used on the second task
        def extract_data(data_interval_end):
            #past the code already made on extract_data_API.py
            
            #In the 'city' field, you can specify the desired location from which to extract data from the API.
            city = 'Boston'
            key = '4YMWX3RZTXW8SMPJRYYYE853U'
            #Combine the information above into the API URL
            URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

            #Reads the file created above
            data = pd.read_csv(URL)
                
            #Whenever passing a variable, use 'f' before the ' to make the format work.
            file_path = f'/home/juliacapelasso/Documents/airflowalura/week={data_interval_end}/'
            
            #create the 'raw_data' file with all the data in the 'file_path' folder
            data.to_csv(file_path + 'raw_data.csv')
            #create a file with only temperature data in the 'file_path' folder
            data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperature.csv')
            #create a file with just the day description in the 'file_path' folder
            data[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')



        task_2 = PythonOperator(
            task_id = 'extract_data',
            python_callable= extract_data,
            #dictionare and after ":" you define the jinja template you want to use
            op_kwargs= {'data_interval_end' : '{{data_interval_end.strftime("%Y-%m-%d")}}'}
        )

        task_1>>task_2