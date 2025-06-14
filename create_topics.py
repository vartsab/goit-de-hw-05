from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='denys_admin'
)

topics = [
    NewTopic(name="building_sensors_denys", num_partitions=1, replication_factor=1),
    NewTopic(name="temperature_alerts_denys", num_partitions=1, replication_factor=1),
    NewTopic(name="humidity_alerts_denys", num_partitions=1, replication_factor=1)
]

admin_client.create_topics(new_topics=topics, validate_only=False)

# Перевірка
print("Created topics:")
[print(topic) for topic in admin_client.list_topics() if "denys" in topic]
