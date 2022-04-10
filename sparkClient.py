import findspark
findspark.init()
import pyspark

# import necessary packages
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc



sc = SparkContext()
# we initiate the StreamingContext with 100 second batch interval. #next we initiate our sqlcontext
ssc = StreamingContext(sc, 100)

socket_stream = ssc.socketTextStream("127.0.0.1", 5556)
lines = socket_stream.window( 100 )


#ENTER THE LOGIC FOR GETTING COUNTS OF TWEETS HERE
data = lines.flatMap(lambda x: x.split(' '))
map = data.map(lambda x: (x, 1))
mapreduce = map.reduceByKey(lambda x,y: x+y)


#saves the RDD in a text file
mapreduce.saveAsTextFiles("sample.txt")

'''
words = lines.flatMap(lambda line: line.split(" "))
# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval
'''

#start the streaming
ssc.start()
ssc.awaitTermination()
