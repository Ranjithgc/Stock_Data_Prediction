"""
@Author: Ranjith G C
@Date: 2021-09-12
@Last Modified by: Ranjith G C
@Last Modified time: 2021-09-12 
@Title : Program Aim is to work with Stock live data using kafkaconsumer
"""
from kafka import KafkaConsumer

consumer = KafkaConsumer('testTopic')
for message in consumer:
    values = message.value.decode('utf-8')
    with open('data.csv', 'at') as f:    
        f.write(f"{values}\n")
