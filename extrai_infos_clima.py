import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

#date interval
date_begin = datetime.today()
date_end = date_begin + timedelta(days=7)

#formatting date
date_begin = date_begin.strftime('%Y-%m-%d')
date_end = date_end.strftime('%Y-%m-%d')

city = 'boston'
key = 'H4UGZQL7D97Q7C777MBLV5QYE'

URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{date_begin}/{date_end}?unitGroup=metric&include=days&key={key}&contentType=csv')

data = pd.read_csv(URL)
print(data.head())

file_path = f'/Users/artemisiaweyl/Documents/studied scripts/airflow/datapipeline/semana={date_begin}'
os.mkdir(file_path)

data.to_csv(file_path + 'raw_data.csv')
data[['datetime','tempmin','temp','tempmax']].to_csv(file_path + 'temperature.csv')
data[['datetime','description','icon']].to_csv(file_path + 'conditions.csv')
