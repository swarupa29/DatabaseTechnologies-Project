import kafka
from json import loads
from time import sleep
from json import dumps
import kafka
import socket
consumer = kafka.KafkaConsumer(
    'music',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
print("here1")
for message in consumer:
    message = message.value
    print(message)
    #data ="{"topic":"music","count":"1"}"