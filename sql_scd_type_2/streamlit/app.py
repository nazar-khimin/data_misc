import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide")
st.title("SCD Type 2 Visualization")

# DuckDB in-memory
con = duckdb.connect(database=':memory:')

# Create Tables
con.execute("""
create or replace table delta as (
    select 1 id, 190 val, date'2022-06-15' fact_date union all
    select 1, 18 val, date'2022-06-01' union all -- dup scd
    select 1, 176 val, date'2022-05-30' union all
    select 1, 174 val, date'2022-05-30' union all -- dup delta
    select 1, 172 val, date'2022-05-28' union all
    select 1, 16 val, date'2022-05-24' union all -- dup scd
    select 1, 150 val, date'2022-05-20' union all
    select 1, 15 val, date'2022-05-15' union all -- dup scd
    select 1, 145 val, date'2022-05-13' union all
    select 1, 140 val, date'2022-05-13' union all -- dup delta
    select 1, 120 val, date'2022-04-01' union all
    select 1, 12 val, date'2022-04-01' union all -- dup scd
    select 1, 100 val, date'2022-03-01'
);

create or replace table scd as (
    select 1 id, 18 val, date'2022-06-01' effective_from, date'9999-12-31' effective_to, timestamp'2023-01-01' snapshot_update_ts union all
    select 1, 17 val, date'2022-05-26', date'2022-05-31', timestamp'2023-01-01' union all
    select 1, 16 val, date'2022-05-24', date'2022-05-25', timestamp'2023-01-01' union all
    select 1, 15 val, date'2022-05-15', date'2022-05-23', timestamp'2023-01-01' union all
    select 1, 14 val, date'2022-05-01', date'2022-05-14', timestamp'2023-01-01' union all
    select 1, 13 val, date'2022-04-15', date'2022-04-30', timestamp'2023-01-01' union all
    select 1, 12 val, date'2022-04-01', date'2022-04-14', timestamp'2023-01-01' union all
    select 1, 11 val, date'2022-03-15', date'2022-03-31', timestamp'2023-01-01' union all
    select 2, 22, date'2022-01-15', date'9999-12-31', timestamp'2023-01-01' union all
    select 3, 33, date'2022-01-15', date'9999-12-31', timestamp'2023-01-01'
);
""")

st.success("✅ `delta` and `scd` tables created in DuckDB")

# Show the top few records
st.subheader("Preview: delta")
st.dataframe(con.execute("SELECT * FROM delta ORDER BY fact_date DESC LIMIT 10").df(), use_container_width=True)

st.subheader("Preview: scd")
st.dataframe(con.execute("SELECT * FROM scd WHERE id = 1 ORDER BY effective_from DESC LIMIT 10").df(), use_container_width=True)