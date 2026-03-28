import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# ==============================
# 🚀 SPARK SESSION
# ==============================
spark = SparkSession.builder \
    .appName("BronzeLayerWriter") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================
# 📡 READ FROM KAFKA
# ==============================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "retail-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# ==============================
# 🔄 CONVERT BYTES → STRING
# ==============================
df = df.selectExpr("CAST(value AS STRING)")

# ==============================
# 🧬 DEFINE SCHEMA
# ==============================
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

# ==============================
# 🔍 PARSE JSON
# ==============================
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# ==============================
# 🥉 WRITE TO BRONZE LAYER
# Raw data — no transformations
# Partitioned by country
# ==============================

BRONZE_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\data_lake\bronze"
CHECKPOINT_PATH = r"C:\tmp\checkpoints\bronze"

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", BRONZE_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .partitionBy("country") \
    .start()

print("✅ Bronze layer writer running...")
print(f"📂 Writing to {BRONZE_PATH}")

query.awaitTermination()