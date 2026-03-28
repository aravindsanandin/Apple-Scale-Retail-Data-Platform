import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# ==============================
# 🚀 SPARK SESSION
# ==============================
spark = SparkSession.builder \
    .appName("SilverLayerWriter") \
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

df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# ==============================
# 🥈 SILVER TRANSFORMATIONS
# ==============================

# 1. Fix timestamp
df = df.withColumn("event_time", to_timestamp(col("event_time")))

# 2. Add processing time
df = df.withColumn("processed_time", current_timestamp())

# 3. Remove nulls
df = df.filter(col("transaction_id").isNotNull())
df = df.filter(col("product_name").isNotNull())

# 4. Remove fraud
df = df.filter(col("unit_price") > 0)
df = df.filter(col("total_amount") > 0)
df = df.filter(col("unit_price") < 10000000)

# 5. Ensure total_amount is correct
df = df.filter(col("quantity") > 0)

SILVER_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\data_lake\silver"
CHECKPOINT_PATH = r"C:\tmp\checkpoints\silver"

# ==============================
# 🥈 WRITE TO SILVER LAYER
# ==============================
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", SILVER_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .partitionBy("country") \
    .start()

print("✅ Silver layer writer running...")
print(f"📂 Writing to {SILVER_PATH}")

query.awaitTermination()