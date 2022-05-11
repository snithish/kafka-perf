from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[3]").appName("KafkaProducer").getOrCreate() 

from typing import Iterator
def p(itr: Iterator) -> int:
    from confluent_kafka import Producer
    import socket

    conf = {'bootstrap.servers': "0.0.0.0:9092",
        'client.id': socket.gethostname()}
    ack_counter = 0
    def acked(err, msg):
        nonlocal ack_counter
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            ack_counter = ack_counter + 1

    producer = Producer(conf)
    poll_counter = 0
    for e in itr:
        producer.produce("quickstart", value=str(e), callback=acked)
        # print(f"produced: {e}")
        if poll_counter > 100:
            producer.flush()
            poll_counter = 0

    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    print("Flushing")
    producer.flush()
    print(f"ack_counter: {ack_counter}")
    return [ack_counter]

def main():
    from pyspark import SparkConf
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[3]").appName("KafkaProducer").getOrCreate()
    df = spark.read.option("header", "true").csv("fhvhv_tripdata_2022-02.csv")
    dfr = df.limit(10000000).repartition(100)
    print(dfr.count())
    res = dfr.toJSON().mapPartitions(p).collect()
    print(res)
    print(sum(res))


if __name__ == "__main__":
    main()