import findspark
findspark.init()
import pyspark
import json
# import necessary packages
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

def res(y):
    return y

sc = SparkContext()
# we initiate the StreamingContext with 100 second batch interval. #next we initiate our sqlcontext
ssc = StreamingContext(sc, 30)

socket_stream = ssc.socketTextStream("127.0.0.1", 5556)
lines = socket_stream.window( 30 )

#ENTER THE LOGIC FOR GETTING COUNTS OF TWEETS HERE

#get tweets with music
filter1=lines.filter(lambda line: line if "#music" in line.lower() else None)
map = filter1.map( lambda line: ( 'music', 1)  )
mapreduce = map.reduceByKey( lambda a, b: a + b )
final1=mapreduce.map(lambda a: json.dumps(a))
val1=final1.pprint()
print(val1)

repartitioned = mapreduce.repartition(1)

#get tweets with ipl
filter2=lines.filter(lambda line: line if "#ipl" in line.lower() else None)
map2 = filter2.map( lambda line: ( 'ipl', 1 ) )
mapreduce2 = map2.reduceByKey( lambda a, b: a + b )
final=mapreduce2.map(lambda a: json.dumps(a))

mapreduce2.pprint()
val=final.pprint()


#do the same thing for 2 more topics



#saves the RDD in a text file
'''
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
host = "127.0.0.1"     # Get local machine name
port = 5557        # Reserve a port for your service.
s.bind((host, port))        # Bind to the port
    
print("Listening on port: %s" % str(port))
s.listen(5)                 # Now wait for client connection.
c, addr = s.accept()        # Establish connection with client.
print("Received request from: " + str(addr))
    #print(c)
c_socket=c

repartitioned.saveAsTextFiles("sample.csv")
c_socket.send( repartitioned.encode('utf-8'))

'''

#start the streaming
ssc.start()
ssc.awaitTermination()


