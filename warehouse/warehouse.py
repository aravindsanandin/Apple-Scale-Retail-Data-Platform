import duckdb
import os

# ==============================
# 📁 PATHS
# ==============================
GOLD_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\data_lake\gold"

# ==============================
# 🦆 CONNECT TO DUCKDB
# ==============================
con = duckdb.connect(
    r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\warehouse\retail.duckdb"
)

print("✅ DuckDB connected")

# ==============================
# 📊 CREATE VIEWS FROM GOLD PARQUET
# No loading needed — DuckDB queries Parquet directly
# ==============================
con.execute(f"""
    CREATE OR REPLACE VIEW revenue_by_country AS
    SELECT * FROM read_parquet('{GOLD_PATH}/revenue_by_country/*.parquet')
""")

con.execute(f"""
    CREATE OR REPLACE VIEW top_products AS
    SELECT * FROM read_parquet('{GOLD_PATH}/top_products/*.parquet')
""")

con.execute(f"""
    CREATE OR REPLACE VIEW revenue_by_store AS
    SELECT * FROM read_parquet('{GOLD_PATH}/revenue_by_store/*.parquet')
""")

print("✅ Views created over Gold Parquet files")

# ==============================
# 🔍 QUERY 1 — Revenue by Country
# ==============================
print("\n📊 Revenue by Country:")
print("-" * 60)
result = con.execute("""
    SELECT
        country,
        currency,
        total_transactions,
        ROUND(total_revenue, 2) as total_revenue
    FROM revenue_by_country
    ORDER BY total_transactions DESC
""").fetchdf()
print(result.to_string(index=False))

# ==============================
# 🔍 QUERY 2 — Top 10 Products
# ==============================
print("\n📊 Top 10 Products by Transactions:")
print("-" * 60)
result = con.execute("""
    SELECT
        product_name,
        total_transactions,
        ROUND(total_revenue, 2) as total_revenue
    FROM top_products
    ORDER BY total_transactions DESC
    LIMIT 10
""").fetchdf()
print(result.to_string(index=False))

# ==============================
# 🔍 QUERY 3 — Top Stores
# ==============================
print("\n📊 Top Stores by Revenue:")
print("-" * 60)
result = con.execute("""
    SELECT
        store_id,
        country,
        total_transactions,
        ROUND(total_revenue, 2) as total_revenue
    FROM revenue_by_store
    ORDER BY total_transactions DESC
""").fetchdf()
print(result.to_string(index=False))

# ==============================
# 🔍 QUERY 4 — Business Insights
# ==============================
print("\n📊 Key Business Insights:")
print("-" * 60)

total_transactions = con.execute("""
    SELECT SUM(total_transactions) FROM revenue_by_country
""").fetchone()[0]

total_countries = con.execute("""
    SELECT COUNT(DISTINCT country) FROM revenue_by_country
""").fetchone()[0]

top_product = con.execute("""
    SELECT product_name FROM top_products
    ORDER BY total_transactions DESC LIMIT 1
""").fetchone()[0]

top_store = con.execute("""
    SELECT store_id FROM revenue_by_store
    ORDER BY total_transactions DESC LIMIT 1
""").fetchone()[0]

print(f"  Total Transactions processed : {total_transactions:,}")
print(f"  Total Countries              : {total_countries}")
print(f"  Best Selling Product         : {top_product}")
print(f"  Busiest Store                : {top_store}")

print("\n🎉 DuckDB Warehouse complete!")
con.close()