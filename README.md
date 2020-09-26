# FinalProj

##Bitcoin Transaction Streaming
A real time bitcoin transactions streaming project by Kafka, pyspark and hive

It receives the stream of bitcoin transaction data from the API below. 
https://www.blockchain.com/zh-cn/api/api_websocket

Transaction log is streamed to kafka in a mini-batch(30s). A kafka consumer will consume the transactions data and 
save the transactions whose size is larger than 300. 

##Prerequisites
python3.5

pip install pykafka

pip install websocket-client

pip install --force-reinstall pyspark==2.4.6

##packages:

spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar

##Deployment

1.Start you screen, go to your kafka path

/yourpath/kafka_2.13-2.6.0

2.Start zookeeper

bin/zookeeper-server-start.sh config/zookeeper.properties

3.Start kafka server 

bin/kafka-server-start.sh config/server.properties

4.Create a topic(test)

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test

bin/kafka-topics.sh --list --bootstrap-server localhost:9092

5.Run producer

python producer.py 

6.Submit job to spark

spark-submit --jars \
spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar \
kafka_bitcoin_hive.py \
localhost:9092 test

7.sample data

please see sample_json

8.presentation video

https://web.microsoftstream.com/video/10265b5e-7aaa-456d-ad41-d4a729fa60f3

