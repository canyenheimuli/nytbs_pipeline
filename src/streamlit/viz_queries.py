# Packages
import socks
import socket
import urllib.parse
from urllib.parse import quote_plus
import certifi
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
import pytds
from datetime import date, datetime, timedelta
import streamlit as st
import pandas as pd

# Create Engine fn
@st.cache_resource
def viz_engine() -> Engine:
    """
    Creates and returns an SQLAlchemy engine connected to the 
    Azure SQL database w/a Fixie SOCKS proxy, pytds driver
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
    socket.setdefaulttimeout(10)  # SOCKS + TCP connect timeout

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
            "cafile": cafile,    # CA file for encryption
            "timeout": 10,       # pytds query timeout
            "login_timeout": 10, # pytds login/auth timeout
        },
        pool_pre_ping=True,      # drops and replaces stale connections
        pool_size=5,             # max persistent connections in the pool
        max_overflow=2,          # extra connections allowed under high load
        pool_timeout=30,         # seconds to wait for a pool connection
        pool_recycle=1800,       # recycle connections every 30 minutes
    )

# Exec Query w Retry Wrapper fn.
def execute_w_retry(engine, query, attempts=5, base_delay=1, max_delay=20) -> Sequence[Row]:
    '''
    Runs the provided query with the provided engine with built-in
    query-level retry logic (recommended by Microsoft)
    '''
    last_err: Exception | None = None
    for attempt in range(attempts):
        try:
            with engine.connect() as conn:
                return conn.execute(query).fetchall()
        except Exception as e:
            last_err = e
            time.sleep(min(base_delay * 2**attempt, max_delay))
    raise last_err

# Get avail weeks in db for week filter
@st.cache_data
def get_avail_weeks(week_cycle_code) -> list[date]:
    '''
    Get (and cache) all unique weeks in
    db to benchmark for week filter
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("SELECT DISTINCT list_date FROM weekly_lists ORDER BY list_date DESC")
    
    # Connect, get weeks
    rows = execute_w_retry(engine, query)

    # Output (list of dates)
    return [r[0] for r in rows]
    
# Get avail months in db for month filter
@st.cache_data
def get_avail_months(month_str) -> list[date]:
    '''
    Get (and cache) all unique months in
    db to benchmark for month filter
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("""
        SELECT DISTINCT 
            list_date_month, 
            list_date_year 
        FROM monthly_lists 
        ORDER BY list_date_year DESC, list_date_month DESC;
    """)
    
    # Connect, get months
    rows = execute_w_retry(engine, query)

    # Output (list of months)
    return [datetime(r[1], r[0], 1) for r in rows]

# Latest Weeklies Fn.
@st.cache_data
def query_latest_weeklies(week_cycle_code) -> pd.DataFrame:
    '''
    Queries DB and caches data for latest weekly lists
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("""
        SELECT 
            w.list_date,
            w.list_id AS list_id,
            l.list_name AS list_name,
            w.book_rank AS rank,
            b.title AS title,
            b.author AS author,
            b.book_image AS image
        FROM books AS b
        LEFT JOIN weekly_lists AS w ON b.isbn13 = w.isbn13
        LEFT JOIN list_info AS l ON w.list_id = l.list_id
        WHERE w.list_date = (SELECT MAX(list_date) FROM weekly_lists)
        ORDER BY l.list_name, rank;
    """)
    
    # Connect, get latest weeklies
    df = execute_w_retry(engine, query)
    
    # Output
    return pd.DataFrame(df)

# Historical Weeklies Fn.
@st.cache_data
def query_hist_weeklies(week_cycle_code) -> pd.DataFrame:
    '''
    Queries DB and caches data for historical weekly lists
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("""
        SELECT 
            w.list_date,
            w.list_id AS list_id,
            l.list_name AS list_name,
            w.book_rank AS rank,
            b.title AS title,
            b.author AS author,
            b.book_image AS image
        FROM books AS b
        LEFT JOIN weekly_lists AS w ON b.isbn13 = w.isbn13
        LEFT JOIN list_info AS l ON w.list_id = l.list_id
        WHERE w.list_date != (SELECT MAX(list_date) FROM weekly_lists)
        ORDER BY l.list_name, rank;
    """)

    # Connect, get hist. weeklies
    df = execute_w_retry(engine, query)
    
    # Output
    return pd.DataFrame(df)

# Latest Monthlies Fn.
@st.cache_data
def query_latest_monthlies(month_str) -> pd.DataFrame:
    '''
    Queries DB and caches data for latest monthly lists
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("""
        SELECT 
            m.list_date_year,
            m.list_date_month,
            m.list_id AS list_id,
            l.list_name AS list_name,
            m.book_rank AS rank,
            b.title AS title,
            b.author AS author,
            b.book_image AS image
        FROM books AS b
        LEFT JOIN monthly_lists AS m ON b.isbn13 = m.isbn13
        LEFT JOIN list_info AS l ON m.list_id = l.list_id
        WHERE m.list_date_year = (SELECT MAX(list_date_year) FROM monthly_lists)
            AND m.list_date_month = (SELECT MAX(list_date_month) FROM monthly_lists WHERE list_date_year = (SELECT MAX(list_date_year) FROM monthly_lists))
        ORDER BY l.list_name, rank;
    """)
    
    # Connect, get latest monthlies
    df = execute_w_retry(engine, query)
    
    # Output
    return pd.DataFrame(df)

# Historical Monthlies Fn.
@st.cache_data
def query_hist_monthlies(month_str) -> pd.DataFrame:
    '''
    Queries DB and caches data for historical monthly lists
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("""
        SELECT 
            m.list_date_year,
            m.list_date_month,
            m.list_id AS list_id,
            l.list_name AS list_name,
            m.book_rank AS rank,
            b.title AS title,
            b.author AS author,
            b.book_image AS image
        FROM books AS b
        LEFT JOIN monthly_lists AS m ON b.isbn13 = m.isbn13
        LEFT JOIN list_info AS l ON m.list_id = l.list_id
        WHERE (m.list_date_year * 100 + m.list_date_month) < (
            SELECT MAX(list_date_year * 100 + list_date_month) FROM monthly_lists
        )
        ORDER BY l.list_name, rank;
    """)

    # Connect, get hist. monthlies
    df = execute_w_retry(engine, query)
    
    # Output
    return pd.DataFrame(df)

# All time Longest-running Weeklies
@st.cache_data
def query_longest_running_weeklies(week_cycle_code) -> pd.DataFrame:
    '''
    Queries DB and caches data for all weekly list
    books ranked by number of weeks on list (ever)
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("""
        WITH w_ranked_by_period AS (
            SELECT 
                b.title,
                b.author,
                b.book_image,
                b.book_descr,
                w.periods_on_list,
                l.list_name,
                w.list_date,
                ROW_NUMBER() OVER (PARTITION BY b.title ORDER BY w.periods_on_list DESC) AS rownum
            FROM weekly_lists AS w
            LEFT JOIN books AS b
                ON w.isbn13 = b.isbn13
            LEFT JOIN list_info as l
                ON w.list_id = l.list_id
        )
        
        SELECT
            title,
            author,
            book_image,
            book_descr,
            periods_on_list,
            list_name,
            list_date
        FROM w_ranked_by_period
        WHERE rownum = 1
        ORDER BY periods_on_list DESC;
    """)
    
    # Connect, get longest running weeklies
    df = execute_w_retry(engine, query)

    # Output
    return pd.DataFrame(df)

# All time Longest-running Monthlies
@st.cache_data
def query_longest_running_monthlies(month_str) -> pd.DataFrame:
    '''
    Queries DB and caches data for all monthly list
    books ranked by number of months on list (ever)
    '''
    # Create engine
    engine = viz_engine()

    # Define query
    query = text("""
        WITH m_ranked_by_period AS (
            SELECT 
                b.title,
                b.author,
                b.book_image,
                b.book_descr,
                m.periods_on_list,
                l.list_name,
                m.list_date,
                ROW_NUMBER() OVER (PARTITION BY b.title ORDER BY m.periods_on_list DESC) AS rownum
            FROM monthly_lists AS m
            LEFT JOIN books AS b
                ON m.isbn13 = b.isbn13
            LEFT JOIN list_info as l
                ON m.list_id = l.list_id
        )
        
        SELECT
            title,
            author,
            book_image,
            book_descr,
            periods_on_list,
            list_name,
            list_date
        FROM m_ranked_by_period
        WHERE rownum = 1
        ORDER BY periods_on_list DESC;
    """)
    
    # Connect, get longest running monthlies
    df = execute_w_retry(engine, query)

    # Output
    return pd.DataFrame(df)
