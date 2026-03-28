import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round

SILVER_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\data_lake\silver"
GOLD_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\data_lake\gold"

# ==============================
# 🚀 SPARK SESSION
# ==============================
spark = SparkSession.builder \
    .appName("GoldLayerWriter") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================
# 📖 READ SILVER PARQUET
# Gold reads from Silver — not from Kafka
# ==============================
df = spark.read.parquet(SILVER_PATH)

print(f"✅ Silver data loaded — {df.count()} clean transactions")

# ==============================
# 🥇 GOLD AGGREGATION 1
# Revenue by country
# ==============================
revenue_by_country = df.groupBy("country", "currency") \
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_transactions")
    ) \
    .orderBy(col("total_transactions").desc())

revenue_by_country.write \
    .mode("overwrite") \
    .parquet(f"{GOLD_PATH}/revenue_by_country")

print("✅ Gold — revenue_by_country written")

# ==============================
# 🥇 GOLD AGGREGATION 2
# Top products by transaction count
# ==============================
top_products = df.groupBy("product_name") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        round(sum("total_amount"), 2).alias("total_revenue")
    ) \
    .orderBy(col("total_transactions").desc())

top_products.write \
    .mode("overwrite") \
    .parquet(f"{GOLD_PATH}/top_products")

print("✅ Gold — top_products written")

# ==============================
# 🥇 GOLD AGGREGATION 3
# Revenue by store
# ==============================
revenue_by_store = df.groupBy("store_id", "country") \
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_transactions")
    ) \
    .orderBy(col("total_revenue").desc())

revenue_by_store.write \
    .mode("overwrite") \
    .parquet(f"{GOLD_PATH}/revenue_by_store")

print("✅ Gold — revenue_by_store written")

# ==============================
# 🥇 PREVIEW RESULTS
# ==============================
print("\n📊 Revenue by Country:")
revenue_by_country.show()

print("\n📊 Top Products:")
top_products.show()

print("\n📊 Revenue by Store:")
revenue_by_store.show()

print("\n🎉 Gold layer complete!")