from time import sleep
from json import dumps
import kafka
import socket
from json import loads
import json

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 5558))


producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
print("here1")
while True:
    print("here2")
    data = ''
    while True:
        data=''
        msg = s.recv(1024)
        print("here3")
        if len(msg) <= 0:
            break
        data += msg.decode("utf-8") 
        val=json.loads(json.loads(data))
        print(data)
        print(val)
        print(val['topic'])
        topic=val['topic'].replace('#','')
        producer.send(topic, value=data)




