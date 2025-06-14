from kafka import KafkaConsumer
from config import *
import json

consumer = KafkaConsumer(
    TOPIC_TEMP_ALERTS,
    TOPIC_HUMIDITY_ALERTS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("Слухаємо алерти...")
for msg in consumer:
    print("Алерт:", msg.value)