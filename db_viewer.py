import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="DuckDB Viewer", layout="wide")

st.title("📊 DuckDB Data Viewer")

DB_PATH = "payment_allocation_read.duckdb"


# Connect to DuckDB
conn = duckdb.connect(DB_PATH, read_only=True)

# Get tables and views
objects = conn.execute("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_type, table_name
""").fetchdf()

st.sidebar.header("Tables & Views")

selected = st.sidebar.selectbox(
    "Select a table or view",
    objects["table_name"].tolist()
)

if selected:
    df = conn.execute(f"SELECT * FROM {selected} LIMIT 1000").fetchdf()
    st.subheader(f"Preview: {selected}")
    st.dataframe(df, use_container_width=True)

conn.close()
