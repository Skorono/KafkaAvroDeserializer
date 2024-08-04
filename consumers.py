import os

from kafka import KafkaConsumer

from deserializers import KafkaAvroDeserializer

bootstrap_servers = ['localhost:9092']
topic_name = 'chart_topic'
group_id = 'my_group'
consumer = KafkaConsumer(topic_name, group_id=group_id, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

deserializer = KafkaAvroDeserializer()
deserializer.load_schema(os.path.join("schemes", "logs.avsc"))

for message in consumer:
    json = deserializer.deserialize(message.value)
    print(f'Received message: {json}')