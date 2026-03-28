import duckdb
import os

# ==============================
# 📁 PATHS
# ==============================
GOLD_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\data_lake\gold"
EXPORT_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\dashboard\data"

# Create export folder
os.makedirs(EXPORT_PATH, exist_ok=True)

# ==============================
# 🦆 CONNECT TO DUCKDB
# ==============================
con = duckdb.connect(
    r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\warehouse\retail.duckdb"
)

print("✅ DuckDB connected")

# ==============================
# 📤 EXPORT 1 — Revenue by Country
# ==============================
con.execute(f"""
    COPY (
        SELECT * FROM read_parquet('{GOLD_PATH}/revenue_by_country/*.parquet')
        ORDER BY total_transactions DESC
    ) TO '{EXPORT_PATH}/revenue_by_country.csv' (HEADER, DELIMITER ',')
""")
print("✅ Exported revenue_by_country.csv")

# ==============================
# 📤 EXPORT 2 — Top Products
# ==============================
con.execute(f"""
    COPY (
        SELECT * FROM read_parquet('{GOLD_PATH}/top_products/*.parquet')
        ORDER BY total_transactions DESC
    ) TO '{EXPORT_PATH}/top_products.csv' (HEADER, DELIMITER ',')
""")
print("✅ Exported top_products.csv")

# ==============================
# 📤 EXPORT 3 — Revenue by Store
# ==============================
con.execute(f"""
    COPY (
        SELECT * FROM read_parquet('{GOLD_PATH}/revenue_by_store/*.parquet')
        ORDER BY total_transactions DESC
    ) TO '{EXPORT_PATH}/revenue_by_store.csv' (HEADER, DELIMITER ',')
""")
print("✅ Exported revenue_by_store.csv")

print(f"\n🎉 All CSVs exported to {EXPORT_PATH}")
print("📊 Ready to load in Power BI!")

con.close()