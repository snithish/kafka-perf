from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    row_counter = 0
    for e in itr:
        producer.produce("quickstart", value=str(e), callback=acked)
        row_counter += 1
        poll_counter += 1
        # print(f"produced: {e}")
        if poll_counter > 1000:
            print("Flushing")
            producer.flush()
            poll_counter = 0

    print("Flushing")
    producer.flush()
    print(f"ack_counter: {ack_counter}")
    assert row_counter == ack_counter, "Not al messages have been sent"
    return [ack_counter]

def main():
    spark = SparkSession.builder.master("local[3]").appName("KafkaProducer").getOrCreate()
    df = spark.read.option("header", "true").csv("fhvhv_tripdata_2022-02.csv")
    hash_cols = df.columns + ["rand"]
    df = (df.withColumn("rand", F.rand(seed=42) * 30000000)
            .withColumn("hash", F.hash(*hash_cols)))
    dfr = df.limit(100000000).repartition(100)
    df_count = dfr.count()
    assert df_count == dfr.distinct().count(), "Duplicate hash"
    print(dfr.count())
    res = dfr.toJSON().mapPartitions(p).collect()
    print(f"Size of partitions sent to kafka: {res}")
    print(f"Total number of records sent: {sum(res)}")


if __name__ == "__main__":
    main()