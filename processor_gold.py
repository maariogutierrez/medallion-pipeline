import json
import psycopg2
from datetime import datetime
from confluent_kafka import Consumer

conn = psycopg2.connect(
    host="localhost",
    database="db",
    user="admin",
    password="admin"
)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS performance (
        student_id INT PRIMARY KEY,
        gender VARCHAR(50),
        study_time_hours FLOAT,
        attendance_percent FLOAT,
        sleep_hours FLOAT,
        parental_education VARCHAR(100),
        internet_access BOOLEAN,
        extracurricular_activities BOOLEAN,
        part_time_job BOOLEAN,
        previous_grade FLOAT,
        final_exam_score FLOAT,
        final_grade VARCHAR(5),
        score_delta FLOAT,
        points_per_hour FLOAT,
        is_at_risk BOOLEAN,
        timestamp TIMESTAMP
    )
""")
conn.commit()

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'gold-processor-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['data-silver'])

print("Listening for silver data to write to PostgreSQL... Press Ctrl+C to stop.")

def parse_bool(val):
    return str(val).strip().lower() == 'yes'

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None or msg.error():
            continue
            
        silver_data = json.loads(msg.value().decode('utf-8'))
        
        student_id = int(silver_data['student_id'])
        gender = silver_data.get('gender')
        study_time_hours = float(silver_data.get('study_time_hours', 0.0))
        attendance_percent = float(silver_data.get('attendance_percent', 0.0))
        sleep_hours = float(silver_data.get('sleep_hours', 0.0))
        parental_education = silver_data.get('parental_education')
        
        internet_access = parse_bool(silver_data.get('internet_access'))
        extracurricular_activities = parse_bool(silver_data.get('extracurricular_activities'))
        part_time_job = parse_bool(silver_data.get('part_time_job'))
        
        previous_grade = float(silver_data.get('previous_grade', 0.0))
        final_exam_score = float(silver_data.get('final_exam_score', 0.0))
        final_grade = silver_data.get('final_grade')
        
        score_delta = final_exam_score - previous_grade
        points_per_hour = final_exam_score / study_time_hours if study_time_hours > 0 else 0.0
        
        is_at_risk = (
            attendance_percent < 80.0 or
            previous_grade < 50.0 or
            final_exam_score < 50.0 or
            score_delta < -10.0
        )
        
        timestamp = datetime.now().isoformat()
        
        cursor.execute("""
            INSERT INTO performance (
                student_id, gender, study_time_hours, attendance_percent, sleep_hours, 
                parental_education, internet_access, extracurricular_activities, part_time_job, 
                previous_grade, final_exam_score, final_grade, score_delta, points_per_hour, 
                is_at_risk, timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (student_id) DO NOTHING;
        """, (
            student_id, gender, study_time_hours, attendance_percent, sleep_hours,
            parental_education, internet_access, extracurricular_activities, part_time_job,
            previous_grade, final_exam_score, final_grade, score_delta, points_per_hour,
            is_at_risk, timestamp
        ))
        
        conn.commit()
        if cursor.rowcount == 1:
            print(
                f"Gold Row Inserted | Student: {student_id} "
                f"| Risk: {is_at_risk} | Delta: {score_delta:.1f}"
            )
        else:
            print(f"Gold Row Skipped | Student: {student_id} already exists")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cursor.close()
    conn.close()