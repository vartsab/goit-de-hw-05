import random
import json
import time
from kafka import KafkaProducer
from config import *
from datetime import datetime

sensor_id = str(random.randint(1000, 9999))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        'sensor_id': sensor_id,
        'timestamp': datetime.utcnow().isoformat(),
        'temperature': round(random.uniform(25, 45), 2),
        'humidity': round(random.uniform(15, 85), 2)
    }
    producer.send(TOPIC_SENSORS, value=data)
    print("Надіслано:", data)
    time.sleep(2)
