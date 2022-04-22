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
            password="putyours",
            port=5432,
            # attempt to connect for 3 seconds then raise exception
            connect_timeout=3
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
        except (Exception, Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()
        # close the cursor and connection
        cur.close()
        conn.close()


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
    jsonobj = json.parse(message)
    print(message)
    print(jsonobj)
    # data ="{"topic":"music","count":"1"}"
    sql_string = 'INSERT INTO kafka_data(topic_name,count) VALUES(' + \
        jsonobj.topic+','+jsonobj.count+');'
    insert_into_postgres(sql_string)
