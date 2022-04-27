import kafka
import json
from json import loads
from time import sleep
from json import dumps
import socket
from psycopg2 import connect, Error


def insert_into_postgres(sql_string):
    try:
        # declare a new PostgreSQL connection object
        conn = connect(
            dbname="kafka_data",
            user="postgres",
            host="localhost",
            password="root",
            port=5432,
            # attempt to connect for 6 seconds then raise exception
            connect_timeout=6

        )

        cur = conn.cursor()
        print("\ncreated cursor object:", cur)

    except (Exception, Error) as err:
        print("\npsycopg2 connect error:", err)
        conn = None
        cur = None
    if cur != None:
        try:
            cur.execute(sql_string)
            conn.commit()
            print('\nfinished inserting!')
            return
        except (Exception, Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()
        # close the cursor and connection
        cur.close()
        conn.close()



#kafka consumer 
consumer = kafka.KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', #if no commit, rollback to earlier offset
    enable_auto_commit=True,
    group_id='my-group', #group of conusmers
    value_deserializer=lambda x: loads(x.decode('utf-8')))
#subscribe to topics
consumer.subscribe(["covid","elonmusk","sports","johnnydepp","ipl"])

for message in consumer:
    message = message.value
    print(message)
    val=json.loads(message)
    #load as dictionary
    jsonobj = json.loads(val)
    # data ="{"topic":"music","count":"1"}"
    #create a sql command as string
    sql_string = 'INSERT INTO kafka_stream(topic_name,count) VALUES(\'' + \
        str(jsonobj['topic'])+'\','+str(jsonobj['count'])+');'
    insert_into_postgres(sql_string)
    continue




