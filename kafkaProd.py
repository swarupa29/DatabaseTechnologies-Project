from time import sleep
from json import dumps
import kafka
import socket
from json import loads
import json

#create a socket and connect with sparkClient to recieve counts of hashtags
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 5558))

#kafka producer
producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
while True:
    data=''
    #wait till data is got 
    msg = s.recv(1024)
    print("message recieved")
    if len(msg) <= 0:
        break
    data += msg.decode("utf-8")
    #convert to dictionary 
    val=json.loads(json.loads(data))
    print(val['topic'])
    #remove # from topic as kafka topics cannot start with a #
    topic=val['topic'].replace('#','')
    #send to broker
    producer.send(topic, value=data)




