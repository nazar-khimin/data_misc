import streamlit as st
import duckdb
import pandas as pd
import time
import queries  # import the queries module

# Page config
st.set_page_config(page_title="SCD Type 2 Visualization", layout="wide")
st.title("SCD Type 2 Demo in Streamlit")

# Database connection (in-memory)
@st.cache_resource
def init_db():
    return duckdb.connect(database=':memory:')

con = init_db()

# Aggregate queries into list with labels and target tables
sql_commands = [
    ("Drop tables", queries.drop_tables_sql(), None),
    ("Create delta table", queries.create_delta_sql(), "delta"),
    ("Create initial scd table", queries.create_scd_initial_sql(), "scd"),
    ("Insert history client 1", queries.insert_history_1_sql(), "scd"),
    ("Insert history clients 2 & 3", queries.insert_history_23_sql(), "scd"),
    ("Merge option 1 - Dedup Lef Join Full Recalculation", queries.merge_option_1_query(), None),
    ("Merge option 2 - Full Recalculation", queries.merge_option_2_query(), None),
    ("Merge option 3 - Category Insert Optimized", queries.merge_option_3_query(), None),
]

# Dropdown to select query
options = [name for name, _, _ in sql_commands]
selection = st.selectbox("Choose SQL command:", options)

# Find selected SQL and target table
selected_sql, target_table = next((sql, table) for name, sql, table in sql_commands if name == selection)

# Display selected query
st.subheader(selection)
st.code(selected_sql, language='sql')

# Button to execute manually
if st.button(f"Run '{selection}'"):
    try:
        start_time = time.perf_counter()
        result = con.execute(selected_sql)
        end_time = time.perf_counter()
        duration_sec = end_time - start_time

        # Show results or row counts
        if selection.lower().startswith('run') or selected_sql.strip().lower().startswith(('select', 'with')):
            df = result.df()
            st.dataframe(df)
        elif target_table:
            count_df = con.execute(f"SELECT COUNT(*) AS count FROM {target_table}").df()
            st.dataframe(count_df)
            st.success(f"Executed '{selection}' successfully. Row count: {count_df.iloc[0, 0]}")
        else:
            st.success(f"Executed '{selection}' successfully.")

        st.info(f"Query execution time: {duration_sec:.3f} seconds")
    except Exception as e:
        st.error(f"Error in '{selection}': {e}")

# Show count of rows in delta table immediately
try:
    start_time = time.perf_counter()
    count_delta = con.execute("SELECT COUNT(*) AS count FROM delta").fetchone()[0]
    end_time = time.perf_counter()
    duration_sec = end_time - start_time

    st.info(f"Delta table row count: {count_delta} (Query time: {duration_sec:.3f} seconds)")
except Exception as e:
    st.error(f"Error counting rows in delta: {e}")

# Show count of rows in scd table immediately
try:
    start_time = time.perf_counter()
    count_scd = con.execute("SELECT COUNT(*) AS count FROM scd").fetchone()[0]
    end_time = time.perf_counter()
    duration_sec = end_time - start_time

    st.info(f"SCD table row count: {count_scd} (Query time: {duration_sec:.3f} seconds)")
except Exception as e:
    st.error(f"Error counting rows in scd: {e}")
