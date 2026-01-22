import streamlit as st
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

@st.cache_resource
def init_snowflake_connection():
    # Load the private key from the path in secrets
    with open(st.secrets["snowflake"]["private_key_file"], "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None, # Or your key password if you set one
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        private_key=pkb,  # Use the processed key instead of password
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

@st.cache_data(ttl=600)
def run_query(query):
    """Run a query and return results as a pandas DataFrame."""
    conn = init_snowflake_connection()
    return pd.read_sql(query, conn)