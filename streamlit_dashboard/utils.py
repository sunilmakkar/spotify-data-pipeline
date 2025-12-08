import streamlit as st
import snowflake.connector
import pandas as pd

@st.cache_resource
def init_snowflake_connection():
    """Initialize Snowflake connection using secrets."""
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

@st.cache_data(ttl=600)
def run_query(query):
    """Run a query and return results as a pandas DataFrame."""
    conn = init_snowflake_connection()
    return pd.read_sql(query, conn)