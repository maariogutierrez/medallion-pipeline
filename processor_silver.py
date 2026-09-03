import json
from datetime import datetime
from confluent_kafka import Consumer, Producer
import pandas as pd

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'silver-processor-group', 
    'auto.offset.reset': 'earliest'        
})
producer = Producer({'bootstrap.servers': 'localhost:9092'})

consumer.subscribe(['data-bronze'])
silver_topic = 'data-silver'

rows_parsed = 0
rows_dropped = 0

print("Listening for bronze data... Press Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(1.0) 
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        raw_data = json.loads(msg.value().decode('utf-8'))
        data = raw_data.copy()

        dropped = False
        
        for k, v in data.items():
            if pd.notnull(v):
                if k == 'student_id':
                    data[k] = int(v)
            else:
                dropped = True
                rows_dropped += 1
                rows_parsed += 1
                print(f"Dropped row due to missing value: {k}")
                print(f"Percentage of rows dropped: {rows_dropped / rows_parsed * 100:.2f}%")
                break

        if not dropped:
            rows_parsed += 1
            producer.produce(silver_topic, value=json.dumps(data).encode('utf-8'))
        producer.poll(0) 
        
        print(f"Bronze In: {raw_data}")
        print(f"Silver Out: {data if not dropped else 'dropped row'}\n")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.flush()