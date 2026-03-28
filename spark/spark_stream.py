import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("AppleRetailStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "retail-transactions") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("event_time", StringType()) \
    .add("store_id", StringType()) \
    .add("product_name", StringType()) \
    .add("unit_price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("total_amount", DoubleType()) \
    .add("country", StringType()) \
    .add("currency", StringType())

df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "C:/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()