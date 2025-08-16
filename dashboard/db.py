# db.py
from sqlalchemy import create_engine
import streamlit as st
import os

# Use st.cache_resource so connection is reused and not recreated every run
@st.cache_resource
def get_db_connection():
    DB_URL = os.getenv(
        'POSTGRES_URL', 
        'postgresql+psycopg2://postgres:postgres@postgres:5432/price_intelligence'
    )
    return create_engine(DB_URL)