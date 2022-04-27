import findspark
findspark.init()
import pyspark
import socket
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

def sendtheresult( rdd ):
    if not rdd.isEmpty():
        print("in send")
        res=rdd.toDF( [ "topic", "count" ] ).toJSON().first() #{"topic":"music","count":1}
        print( res)
        send_kafka(res)

def send_kafka(res):
    jsonval=json.dumps(res)
    c_socket.send( jsonval.encode('utf-8'))


#spark
sc = SparkContext()
spark = SparkSession(sc)
#initiate the StreamingContext with 30 second batch interval.
# streamingcontext batch interval size
ssc = StreamingContext(sc, 30)
socket_stream = ssc.socketTextStream("127.0.0.1", 5556)
#socket stream window size
lines = socket_stream.window( 30 )

#socket to send to kafka
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)     
host = "127.0.0.1"     
port = 5558        
s.bind((host, port))        
print("Listening on port: %s" % str(port))
s.listen(5)                 
c, addr = s.accept()       
print("Received request from: " + str(addr))
c_socket=c


def get_tweets(lines,topic):
    filter1=lines.filter(lambda line: line if topic in line.lower() else None)
    map = filter1.map( lambda line: ( topic, 1)  )
    mapreduce = map.reduceByKey( lambda a, b: a + b )
    repartitioned = mapreduce.repartition(1)
    #for each {topic,count} send it to kafka
    repartitioned.foreachRDD(sendtheresult)


#to get the count of each hashtag and send each one to kafka
get_tweets(lines,'#covid')
get_tweets(lines,'#elonmusk')
get_tweets(lines,'#sports')
get_tweets(lines,'#ipl')
get_tweets(lines,'#johnnydepp')

#start the streaming

ssc.start()
ssc.awaitTermination()


