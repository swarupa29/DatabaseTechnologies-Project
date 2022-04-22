from time import sleep
from json import dumps
import kafka
import socket
from json import loads
import json

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 5557))


producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
print("here1")
while True:
    print("here2")
    data = ''
    while True:
        msg = s.recv(1024)
        print("here3")
        print(msg)
        if len(msg) <= 0:
            break
        data += msg.decode("utf-8") 
        jsonval=json.loads(data)
        print(jsonval)
        producer.send('music', value=data)


    print("data len and data")
    print(len(data))
    if len(data) > 0:
        print(data)


'''
for e in range(2):
    data = {'number' : e}
    producer.send('music', value=data)
    sleep(5)k
'''



