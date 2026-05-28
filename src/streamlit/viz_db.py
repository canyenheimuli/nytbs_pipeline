# Packages
from typing import Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
import streamlit as st
from datetime import timedelta
# import pandas as pd # Apparently not needed for now

# Get engine for viz queries fn.
@st.cache_resource
def viz_engine() -> Engine:
    '''
    Returns the SQL Alchemy connection engine 
    for connecting to the Azure SQL DB
    '''
    # Params
    server = st.secrets["AZURE_SQL_SERVER"]
    database = st.secrets["AZURE_SQL_DATABASE"]
    username = st.secrets["AZURE_SQL_USERNAME"]
    password = st.secrets["AZURE_SQL_PASSWORD"]
    driver = "{ODBC Driver 18 for SQL Server}"

    # Create string and connect using engine
    conn_string = f"Driver={driver}; \
                  Server=tcp:{server},1433; \
                  Database={database}; \
                  Uid={username}; \
                  Pwd={password}; \
                  Encrypt=yes; \
                  TrustServerCertificate=no; \
                  Connection Timeout=200; \
                  ConnectRetryCount=3"
    
    conn_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_string)}"
    
    # Output
    return create_engine(conn_url, connect_args={"timeout": 30})

# Weekly lists query fn.
@st.cache_data(ttl=timedelta(days=7))
def query_latest_weeklies(list_id: int) -> list[tuple[Any, ...]]:
    '''
    Queries DB for the latest weekly 
    titles for a supplied list ID
    '''
    # Init engine
    engine = viz_engine()
    
    # Make and run query
    query = text(
        """
        SELECT 
            w.book_rank AS rank,
            b.title AS title,
            b.author AS author,
            b.book_image AS image
        FROM books AS b
        LEFT JOIN weekly_lists AS w
            ON b.isbn13 = w.isbn13
        WHERE w.retrieval_date = (SELECT MAX(retrieval_date) FROM weekly_lists)
            AND w.list_id = :list_id;
        """
    )
    
    with engine.connect() as conn:
        df = conn.execute(query, {"list_id": list_id}).fetchall()
    
    # Output
    return df

# Monthly lists query fn.
@st.cache_data(ttl=timedelta(days=7))
def query_latest_monthlies(list_id: int) -> list[tuple[Any, ...]]:
    '''
    Queries DB for the latest monthly 
    titles for a supplied list ID
    '''
    # Init engine
    engine = viz_engine()
    
    # Make and run query
    query = text(
        """
        SELECT 
            m.book_rank AS rank,
            b.title AS title,
            b.author AS author,
            b.book_image AS image
        FROM books AS b
        LEFT JOIN monthly_lists AS m
            ON b.isbn13 = m.isbn13
        WHERE m.retrieval_date = (SELECT MAX(retrieval_date) FROM monthly_lists)
            AND w.list_id = :list_id;
        """
    )
    
    with engine.connect() as conn:
        df = conn.execute(query, {"list_id": list_id}).fetchall()
    
    # Output
    return df
