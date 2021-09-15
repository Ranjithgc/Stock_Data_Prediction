"""
@Author: Ranjith G C
@Date: 2021-09-12
@Last Modified by: Ranjith G C
@Last Modified time: 2021-09-12 
@Title : Program Aim is to work with Stock live data and producing by using kafkaproducer
"""
import csv
import requests
import os
import time
from kafka import KafkaProducer
from json import dumps 

from dotenv import load_dotenv
load_dotenv('.env')

bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers = bootstrap_servers, value_serializer=lambda K:dumps(K).encode('utf-8'))

key = os.getenv('API_KEY')

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=15min&slice=year1month1&apikey= key'

with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    for row in my_list:
        producer.send('testTopic',row)