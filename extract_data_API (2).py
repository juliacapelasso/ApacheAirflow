import os 
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

#data range:
start_date = datetime.today()
end_date = start_date + timedelta(days=7)

#formatting the dates
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')
print(end_date, start_date)

#In the 'city' field, you can specify the desired location from which to extract data from the API.
city = 'Boston'
key = '4YMWX3RZTXW8SMPJRYYYE853U'
#Combine the information above into the API URL
URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
f'{city}/{start_date}/{end_date}?unityGroup=metric&include=days&key={key}&contentType=csv') 

#Reads the file created above
data = pd.read_csv(URL)
#Print the first 5 rows from the URL file
print(data.head())

#Whenever passing a variable, use 'f' before the ' to make the format work.
#os.mkdir 'os' is the operating system and 'mkdir' is the Linux function to create a directory in the specified path.
file_path = f'/home/juliacapelasso/Documents/DATA_PIPELINE/week-{start_date}/'
os.mkdir(file_path)

#create the 'raw_data' file with all the data in the 'file_path' folder
data.to_csv(file_path + 'raw_data.csv')
#create a file with only temperature data in the 'file_path' folder
data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperature.csv')
#create a file with just the day description in the 'file_path' folder
data[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')