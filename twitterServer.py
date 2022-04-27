from pyspark.sql.functions import desc
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import pyspark
import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
import socket
import json
import findspark
findspark.init()



consumer_key = '5g1ADb49Pt764JHhneJu41HYg'
consumer_secret = 'tzcSj9p19yYk3g6SFRHkhjiwwXU7DNN84uaFKvTZ4CwW8Lse4O'
access_token = '1263725863561908224-mFWeC0uriREhbIAZatHwfH9TQR3jXX'
access_secret = '0O3Fzs2K7TVkx7Xcuqw8dtYz7kjrVY62BgHlPhEanlMiV'

c_socket=None
class TweetsListener(tweepy.Stream):

    
    # override the on_data() function in StreamListener
    
    def on_data(self, data):
        try:
            msg = json.loads( data )
            c_socket.send( msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            return True

    
    def if_error(self, status):
        print(status)
        return True


def send_tweets():
    stream_listener = TweetsListener(consumer_key, consumer_secret, access_token, access_secret)
    stream_listener.filter(track=['#ipl','#johnnydepp','#sports','#elonmusk','#covid'],languages=["en"])


if __name__ == "__main__":

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    host = "127.0.0.1"    
    port = 5556        
    s.bind((host, port))        # Bind port,host
    print("Listening on port: %s" % str(port))
    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.
    print("Received request from: " + str(addr))
    c_socket=c
    #start sending tweets
    send_tweets()


