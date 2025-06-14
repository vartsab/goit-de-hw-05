from kafka import KafkaConsumer, KafkaProducer
from config import *
import json

consumer = KafkaConsumer(
    TOPIC_SENSORS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for msg in consumer:
    data = msg.value
    sensor_id = data['sensor_id']
    temperature = data['temperature']
    humidity = data['humidity']
    timestamp = data['timestamp']

    if temperature > 40:
        alert = {
            'sensor_id': sensor_id,
            'timestamp': timestamp,
            'temperature': temperature,
            'message': 'Температура перевищує норму!'
        }
        producer.send(TOPIC_TEMP_ALERTS, value=alert)
        print("Температурний алерт:", alert)

    if humidity > 80 or humidity < 20:
        alert = {
            'sensor_id': sensor_id,
            'timestamp': timestamp,
            'humidity': humidity,
            'message': 'Вологість поза допустимим діапазоном!'
        }
        producer.send(TOPIC_HUMIDITY_ALERTS, value=alert)
        print("Алерт вологості:", alert)