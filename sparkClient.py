import findspark
findspark.init()
import pyspark

# import necessary packages
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

def res(y):
    return y

sc = SparkContext()
# we initiate the StreamingContext with 100 second batch interval. #next we initiate our sqlcontext
ssc = StreamingContext(sc, 100)

socket_stream = ssc.socketTextStream("127.0.0.1", 5556)
lines = socket_stream.window( 100 )

#ENTER THE LOGIC FOR GETTING COUNTS OF TWEETS HERE
#data = lines.flatMap( lambda text: text.split( " " ) )
#filter=data.filter( lambda word: word.lower().startswith("#") )
#filter=lines.filter(lambda line: line.lower().contains('music'))
map = lines.map( lambda line: ( 'music', 1 ) )
mapreduce = map.reduceByKey( lambda a, b: a + b )
mapreduce.pprint()
repartitioned = mapreduce.repartition(1)
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


