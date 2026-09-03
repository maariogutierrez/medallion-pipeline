"""Publish student performance records from CSV to the bronze Kafka topic."""

import time
import pandas as pd
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = 'data-bronze'

df = pd.read_csv('student_performance_dataset.csv')

print("Starting to send rows every 10 seconds. Press Ctrl+C to stop.")

for index, row in df.iterrows():
    message = row.to_json()
    
    producer.produce(topic, value=message.encode('utf-8'))
    
    producer.flush() 
    
    print(f"Sent: {message}")
    
    time.sleep(10)