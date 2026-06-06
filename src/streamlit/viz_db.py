# Packages
import socks
import socket
import urllib.parse
from urllib.parse import quote_plus
import certifi
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
import pytds
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
    # Proxy (Fixie)
    fixie_url = st.secrets["FIXIE_URL"]
    if not fixie_url:
        raise ValueError("Fixie URL not set in Streamlit secrets")

    if not fixie_url.startswith(("socks5://", "socks4://", "http://")):
        fixie_url = "socks5://" + fixie_url
    
    parsed_url = urllib.parse.urlparse(fixie_url)
    if not all([parsed_url.hostname, parsed_url.port,
                parsed_url.username, parsed_url.password]):
        raise ValueError(f"Fixie URL could not be fully parsed: {fixie_url}")

    # Monkeypatch before engine creation
    socks.set_default_proxy(
        socks.SOCKS5,
        parsed_url.hostname,
        parsed_url.port,
        username=parsed_url.username,
        password=parsed_url.password
    )
    socket.socket = socks.socksocket
    
    # DB Params
    server   = st.secrets["AZURE_SQL_SERVER"]
    database = st.secrets["AZURE_SQL_DATABASE"]
    username = st.secrets["AZURE_SQL_USERNAME"]
    password = st.secrets["AZURE_SQL_PASSWORD"]

    # Connection URL (pytds)
    conn_url = (
        f"mssql+pytds://{quote_plus(username)}:{quote_plus(password)}"
        f"@{server}/{database}?charset=utf8"
    )

    cafile = certifi.where()

    # Output (Engine)
    return create_engine(
        conn_url,
        connect_args={
            "cafile": cafile,   # CA file for encryption
        },
        pool_pre_ping=True,      # drops and replaces stale connections
        pool_size=5,             # max persistent connections in the pool
        max_overflow=2,          # extra connections allowed under high load
        pool_timeout=30,         # seconds to wait for a pool connection
        pool_recycle=1800,       # recycle connections every 30 minutes
    )

# Weekly lists query fn.
@st.cache_data(ttl=timedelta(days=7))
def query_weeklies(date: str = None) -> pd.DataFrame: # TO-DO: Update type hint and control flow with expected date arg structure
    '''
    Queries DB for all weekly lists
    for a given date (latest by default)
    '''
    # Create engine
    engine = viz_engine()

    # Connect, run queries
    with engine.connect() as conn:
        
        # Get date as an object
        if date is None:
            result = conn.execute(text("SELECT MAX(retrieval_date) FROM weekly_lists"))
            date = result.scalar() 

        # Run main query
        query = text("""
            SELECT 
                w.list_id AS list_id,
                l.list_name AS list_name,
                w.book_rank AS rank,
                b.title AS title,
                b.author AS author,
                b.book_image AS image
            FROM books AS b
            LEFT JOIN weekly_lists AS w ON b.isbn13 = w.isbn13
            LEFT JOIN list_info AS l ON w.list_id = l.list_id
            WHERE w.retrieval_date = :date
            ORDER BY l.list_name, rank;
        """)

        df = pd.read_sql(query, conn, params={"date": date})

    # Output
    return df

# Monthly lists query fn.
@st.cache_data(ttl=timedelta(days=7))
def query_monthlies(date: str = None) -> pd.DataFrame: # TO-DO: Update type hint and control flow with expected date arg structure
    '''
    Queries DB for all monthly lists
    for a given date (latest by default)
    '''
    # Create engine
    engine = viz_engine()

    # Connect, run queries
    with engine.connect() as conn:
        
        # Get date as an object
        if date is None:
            result = conn.execute(text("SELECT MAX(retrieval_date) FROM monthly_lists"))
            date = result.scalar() 

        # Run main query
        query = text("""
            SELECT 
                m.list_id AS list_id,
                l.list_name AS list_name,
                m.book_rank AS rank,
                b.title AS title,
                b.author AS author,
                b.book_image AS image
            FROM books AS b
            LEFT JOIN monthly_lists AS m
                ON b.isbn13 = m.isbn13
            LEFT JOIN list_info AS l
                ON m.list_id = l.list_id
            WHERE m.retrieval_date = :date
            ORDER BY l.list_name, rank;
        """)

        df = pd.read_sql(query, conn, params={"date": date})

    # Output
    return df
