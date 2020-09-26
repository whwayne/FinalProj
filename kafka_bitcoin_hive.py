from __future__ import print_function

import sys
import os
import json


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import HiveContext, Row

outputPath = '/tmp/bitcoin'


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = HiveContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def createContext():
    sc = SparkContext(appName="PythonStreamingDirectKafkaBitCoin")
    ssc = StreamingContext(sc, 30)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    #filter the records whose size is larger than 300
    size300 = kvs.filter(lambda x: json.loads(x[1])["x"]["size"] > 300)
    size300.pprint()

    def writeRecord(rdd):
        try:
            hiveContext = getSqlContextInstance(rdd.context)
            rowRdd = rdd.map(lambda x: Row(key=x[0], value=x[1]))
            print(rowRdd.take(2))
            sizeDataFrame = hiveContext.createDataFrame(rowRdd)
            sizeDataFrame.show()
            sizeDataFrame.toDF().registerTempTable("wc")
            hiveContext.sql("use bda")
            hiveContext.sql("INSERT INTO TABLE test SELECT key, value from wc")
        except Exception as e:
            print(str(e))
            pass

    size300.foreachRDD(writeRecord)
    return ssc


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_bitcoin.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)
    else:
        print("Creating new context")
        if os.path.exists(outputPath):
            os.remove(outputPath)

        ssc = StreamingContext.getOrCreate(outputPath, lambda: createContext())
        ssc.start()
        ssc.awaitTermination()