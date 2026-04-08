# Packages
import os
from dotenv import load_dotenv
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus

# Load Env
load_dotenv()

# Get conn string fn.
def get_azuresqldb_engine() -> Engine:
    '''
    Returns the formatted conn string for
    connecting to the specified Azure SQL DB
    '''
    # Conn string params
    server = os.environ.get('SERVER_NAME')
    database = os.environ.get('DB_NAME')
    username = os.environ.get('DB_UID')
    password = os.environ.get('DB_PWD')
    
    # Local Location:
    # driver = '{/usr/local/lib/libmsodbcsql.18.dylib}'
    driver = '{/opt/homebrew/lib/libmsodbcsql.18.dylib}' # UPDATE AS NEEDED
    # Remote/GitHub Actions (?) Location
    # driver = '{{ODBC Driver 18 for SQL Server}}' # UPDATE AS NEEDED
    
    # Create string and connect using engine
    conn_string = f"Driver={driver}; \
                  Server=tcp:{server},1433; \
                  Database={database}; \
                  Uid={username}; \
                  Pwd={password}; \
                  Encrypt=yes; \
                  TrustServerCertificate=no; \
                  Connection Timeout=200;"
    
    conn_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_string)}"
    
    return create_engine(conn_url)

# Conditional table loading function
def insert_tables(data: dict, engine: Engine) -> None:
    '''
    Perform conditional loading of
    tables based on table 
    - Lists: Staged inseration only for new records
    - Books: Staged insertion only for new records
    - Weekly: Normal insertion
    - Monthly: Staged insertion only where staged data is different from most recent data
    '''
    # List info  
    with engine.connect() as engine_conn:
        
        # Set queries
        stage_table_query = "CREATE TABLE list_info_stage (list_id SMALLINT, list_name VARCHAR(100) NOT NULL);"
        
        cond_merge_query = """
        MERGE INTO list_info AS li
        USING list_info_stage AS lis
            ON (li.list_id = lis.list_id)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (list_id, list_name)
            VALUES (lis.list_id, lis.list_name);
        """
        
        del_stage_query = "DROP TABLE list_info_stage;"
        
        # Create stage table, stage data, conditional merge, delete stage, close conn
        engine_conn.execute(text(stage_table_query))
        data['list_info'].to_sql('list_info_stage', con = engine_conn, if_exists = 'append', index = False)
        engine_conn.execute(text(cond_merge_query))
        engine_conn.execute(text(del_stage_query))
        engine_conn.commit()
        engine_conn.close()
    
    print("---List info data loaded to db---")
    
    # Book info
    with engine.connect() as engine_conn:
        
        # Set queries
        stage_table_query = """
        CREATE TABLE books_stage (
            isbn13 VARCHAR(20) NOT NULL,	
            title VARCHAR(100) NOT NULL,	
            author VARCHAR(100) NOT NULL, 
            book_descr VARCHAR(MAX) NULL, 	
            publisher VARCHAR(100) NULL,
            book_image VARCHAR(MAX) NULL
        );
        """
        
        cond_merge_query = """
        MERGE INTO books AS b
        USING books_stage AS bs
            ON (b.isbn13 = bs.isbn13)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (isbn13, title, author, book_descr, publisher, book_image)
            VALUES (bs.isbn13, bs.title, bs.author, bs.book_descr, bs.publisher, bs.book_image);
        """
        
        del_stage_query = "DROP TABLE books_stage;"
        
        # Create stage table, stage data, conditional merge, delete stage, close conn
        engine_conn.execute(text(stage_table_query))
        data['books'].to_sql('books_stage', con = engine_conn, if_exists = 'append', index = False)
        engine_conn.execute(text(cond_merge_query))
        engine_conn.execute(text(del_stage_query))
        engine_conn.commit()
        engine_conn.close()
    
    print("---Books data loaded to db---")
    
    # Weekly lists
    # data['weekly_lists'].to_sql('weekly_lists', con = engine, if_exists = 'replace', index = False)
    
    ### TESTING
    with engine.connect() as engine_conn:
        
        # Set queries
        stage_table_query = """
        CREATE TABLE weekly_stage (
            list_id SMALLINT NOT NULL,
            book_rank SMALLINT NOT NULL,
            isbn13 VARCHAR(20) NOT NULL,	
            rank_last_period SMALLINT NOT NULL,
            periods_on_list SMALLINT NOT NULL, 
            list_date DATE NOT NULL,
            list_date_month SMALLINT NULL,
            list_date_year SMALLINT NULL,
            retrieval_date DATE NOT NULL,
        );
        """
        
        cond_merge_query = """
        MERGE INTO weekly_lists AS w
        USING weekly_stage AS ws
            ON (
                w.list_id = ws.list_id AND
                w.book_rank = ws.book_rank AND
                w.list_date = ws.list_date
                )
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (list_id, book_rank, isbn13, rank_last_period, periods_on_list, list_date, list_date_month, list_date_year, retrieval_date)
            VALUES (ws.list_id, ws.book_rank, ws.isbn13, ws.rank_last_period, ws.periods_on_list, ws.list_date, ws.list_date_month, ws.list_date_year, ws.retrieval_date);
        """
        
        del_stage_query = "DROP TABLE weekly_stage;"
        
        # Create stage table, stage data, conditional merge, delete stage, close conn
        engine_conn.execute(text(stage_table_query))
        data['weekly_lists'].to_sql('weekly_stage', con = engine_conn, if_exists = 'append', index = False)
        engine_conn.execute(text(cond_merge_query))
        engine_conn.execute(text(del_stage_query))
        engine_conn.commit()
        engine_conn.close()
    ### TESTING
    
    print("---Weekly list data loaded to db---")
    
    # Monthly lists
    with engine.connect() as engine_conn:
        
        # Set queries
        stage_table_query = """
        CREATE TABLE monthly_stage (
            list_id SMALLINT NOT NULL,
            book_rank SMALLINT NOT NULL,
            isbn13 VARCHAR(20) NOT NULL,	
            rank_last_period SMALLINT NOT NULL,
            periods_on_list SMALLINT NOT NULL, 
            list_date DATE NOT NULL,
            list_date_month SMALLINT NULL,
            list_date_year SMALLINT NULL,
            retrieval_date DATE NOT NULL,
        );
        """
        
        cond_merge_query = """
        MERGE INTO monthly_lists AS m
        USING monthly_stage AS ms
            ON (
                m.list_id = ms.list_id AND
                m.book_rank = ms.book_rank AND
                m.rank_last_period = ms.rank_last_period AND
                m.periods_on_list = ms.periods_on_list AND
                m.list_date_month = ms.list_date_month AND
                m.list_date_year = ms.list_date_year 
                )
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (list_id, book_rank, isbn13, rank_last_period, periods_on_list, list_date, list_date_month, list_date_year, retrieval_date)
            VALUES (ms.list_id, ms.book_rank, ms.isbn13, ms.rank_last_period, ms.periods_on_list, ms.list_date, ms.list_date_month, ms.list_date_year, ms.retrieval_date);
        """
        
        del_stage_query = "DROP TABLE monthly_stage;"
        
        # Create stage table, stage data, conditional merge, delete stage, close conn
        engine_conn.execute(text(stage_table_query))
        data['monthly_lists'].to_sql('monthly_stage', con = engine_conn, if_exists = 'append', index = False)
        engine_conn.execute(text(cond_merge_query))
        engine_conn.execute(text(del_stage_query))
        engine_conn.commit()
        engine_conn.close()
        
    print("---Monthly list data loaded to db---")
        
# Consolidated loader fn
def load(data: dict) -> None:
    '''
    Gets connection to db and appends
    data to tables (or appends conditionally,
    where appropriate)
    '''
    # Create connection
    print("Connecting to database...")
    engine = get_azuresqldb_engine()
    print("Successfully connected; loading tables...")
    
    # Load all data
    insert_tables(data, engine)
    
    # Status Update
    print('All tables loaded successfully to database')

# Conditional Execution
if __name__ == "__main__":
    load()
