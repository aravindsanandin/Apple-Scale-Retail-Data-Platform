import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import time
import os

# ==============================
# ⚙️ CONFIG
# ==============================
st.set_page_config(
    page_title="Global Retail Data Platform",
    page_icon="🍎",
    layout="wide"
)

GOLD_PATH = r"C:\Users\Aravind\OneDrive\Desktop\apple-retail-data-platform\data_lake\gold"

# ==============================
# 🦆 LOAD DATA FROM DUCKDB
# ==============================
@st.cache_data(ttl=30)  # Refreshes every 30 seconds
def load_data():
    con = duckdb.connect()

    revenue_by_country = con.execute(f"""
        SELECT * FROM read_parquet('{GOLD_PATH}/revenue_by_country/*.parquet')
        ORDER BY total_transactions DESC
    """).fetchdf()

    top_products = con.execute(f"""
        SELECT * FROM read_parquet('{GOLD_PATH}/top_products/*.parquet')
        ORDER BY total_transactions DESC
        LIMIT 10
    """).fetchdf()

    revenue_by_store = con.execute(f"""
        SELECT * FROM read_parquet('{GOLD_PATH}/revenue_by_store/*.parquet')
        ORDER BY total_transactions DESC
    """).fetchdf()

    con.close()
    return revenue_by_country, top_products, revenue_by_store

# ==============================
# 🎨 HEADER
# ==============================
st.title("🍎 Global Retail Data Platform")
st.markdown("**Live analytics dashboard — powered by Kafka → Spark → DuckDB**")
st.divider()

# ==============================
# 🔄 LOAD DATA
# ==============================
revenue_by_country, top_products, revenue_by_store = load_data()

# ==============================
# 📊 KEY METRICS ROW
# ==============================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Transactions",
        value=f"{revenue_by_country['total_transactions'].sum():,}"
    )

with col2:
    st.metric(
        label="Total Countries",
        value=len(revenue_by_country)
    )

with col3:
    st.metric(
        label="Best Selling Product",
        value=top_products.iloc[0]['product_name']
    )

with col4:
    st.metric(
        label="Busiest Store",
        value=revenue_by_store.iloc[0]['store_id']
    )

st.divider()

# ==============================
# 📊 ROW 1 — Country + Products
# ==============================
col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        revenue_by_country,
        x="country",
        y="total_transactions",
        title="Transactions by Country",
        color="country",
        labels={"total_transactions": "Transactions", "country": "Country"}
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        top_products,
        x="total_transactions",
        y="product_name",
        orientation="h",
        title="Top 10 Products by Transactions",
        color="total_transactions",
        color_continuous_scale="blues",
        labels={"total_transactions": "Transactions", "product_name": "Product"}
    )
    fig.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

# ==============================
# 📊 ROW 2 — Stores
# ==============================
fig = px.bar(
    revenue_by_store,
    x="store_id",
    y="total_transactions",
    title="Busiest Stores by Transactions",
    color="country",
    labels={"total_transactions": "Transactions", "store_id": "Store"},
    text="total_transactions"
)
fig.update_traces(textposition="outside")
st.plotly_chart(fig, use_container_width=True)

# ==============================
# 📋 RAW DATA TABLES
# ==============================
st.divider()
st.subheader("📋 Raw Data")

tab1, tab2, tab3 = st.tabs([
    "Revenue by Country",
    "Top Products",
    "Store Performance"
])

with tab1:
    st.dataframe(revenue_by_country, use_container_width=True)

with tab2:
    st.dataframe(top_products, use_container_width=True)

with tab3:
    st.dataframe(revenue_by_store, use_container_width=True)

# ==============================
# 🔄 AUTO REFRESH
# ==============================
st.divider()
st.caption("🔄 Dashboard auto-refreshes every 30 seconds")

time.sleep(30)
st.rerun()

# ==============================
# 🔄 AUTO REFRESH
# ==============================
st.divider()

from datetime import datetime
last_updated = datetime.now().strftime("%H:%M:%S")
st.caption(f"🔄 Last updated at: {last_updated} — refreshes every 30 seconds")

time.sleep(30)
st.rerun()