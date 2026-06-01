# Packages
import urllib.parse
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import timedelta
import streamlit as st
import pandas as pd

# Create Engine fn
@st.cache_resource
def viz_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine connected to the Azure SQL
    database via a Fixie SOCKS proxy, using pytds as the backend driver.
    """
    # Proxy
    proxy_url = st.secrets["FIXIE_URL"]
    if not proxy_url:
        raise ValueError("PROXY_URL not set in Streamlit secrets")
    
    parsed_proxy = urllib.parse.urlparse(proxy_url)
    if not all([parsed_proxy.hostname, parsed_proxy.port,
                parsed_proxy.username, parsed_proxy.password]):
        raise ValueError(f"PROXY_URL could not be fully parsed: {proxy_url}")

    # DB Params
    server = st.secrets["AZURE_SQL_SERVER"]
    database = st.secrets["AZURE_SQL_DATABASE"]
    username = st.secrets["AZURE_SQL_USERNAME"]
    password = st.secrets["AZURE_SQL_PASSWORD"]

    # Connection URL (pytds)
    conn_url = (
        f"mssql+pytds://{quote_plus(username)}:{quote_plus(password)}"
        f"@{server}/{database}?charset=utf8"
    )

    # Engine
    return create_engine(
        conn_url,
        connect_args={
            "proxy_hostname": parsed_proxy.hostname,
            "proxy_port":     parsed_proxy.port,
            "proxy_username": parsed_proxy.username,
            "proxy_password": parsed_proxy.password,
            "timeout":        30,    # seconds before connection attempt fails
            "login_timeout":  30,    # seconds to wait for login
        },
        pool_pre_ping=True,          # drops and replaces stale connections
        pool_size=5,                 # max persistent connections in the pool
        max_overflow=2,              # extra connections allowed under high load
        pool_timeout=30,             # seconds to wait for a pool connection
        pool_recycle=1800,           # recycle connections every 30 minutes
    )

# Weekly lists query fn.
@st.cache_data(ttl=timedelta(days=7))
def query_latest_weeklies(list_id: int) -> pd.DataFrame:
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
    return pd.DataFrame(df)

# Monthly lists query fn.
@st.cache_data(ttl=timedelta(days=7))
def query_latest_monthlies(list_id: int) -> pd.DataFrame:
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
    return pd.DataFrame(df)
