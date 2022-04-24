CREATE DATABASE kafka_data;
\c kafka_data;
create table kafka_and_streamingdata(id PRIMARY KEY SERIAL NOT NULL int,topic_name NOT NULL varchar(255), count int);
