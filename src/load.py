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
      
      # Create stage, load, delete stage, close conn
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
      
      # Create stage, load, delete stage, close conn
      engine_conn.execute(text(stage_table_query))
      data['books'].to_sql('books_stage', con = engine_conn, if_exists = 'append', index = False)
      engine_conn.execute(text(cond_merge_query))
      engine_conn.execute(text(del_stage_query))
      engine_conn.commit()
      engine_conn.close()
  
  print("---Books data loaded to db---")
  
  # Weekly lists
  data['weekly_lists'].to_sql('weekly_lists', con = engine, if_exists = 'append', index = False)

  print("---Weekly list data loaded to db---")

  # Monthly lists

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
