# Packages
import os
from dotenv import load_dotenv
import pyodbc
from sqlalchemy import create_engine, text

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
  conn_string = f'Driver={driver};\
                Server=tcp:{server},1433;\
                Database={database};\
                Uid={username};\
                Pwd={password};\
                Encrypt=yes;\
                TrustServerCertificate=no;\
                Connection Timeout=120;'

  return create_engine(conn_string)

# Conditional table loading function
def insert_tables(data: dict, engine: Engine) -> None:
  '''
  Perform conditional loading of
  tables based on table 
  - Weekly: Normal insertion
  - Monthly: Staged insertion only where staged data is different from most recent data
  - Books: Staged insertion only for new records
  - Lists: Staged inseration only for new records
  '''
  # Weekly lists
  data['weekly_lists'].to_sql('weekly_lists', con = engine, if_exists = 'append', index = False)

  # Monthly lists
  # List info
  # Book info

# Consolidated loader fn
def load(data: dict) -> None:
  '''
  Gets connection to db and appends
  data to tables (or appends conditionally,
  where appropriate)
  '''
  # Create connection
  engine = get_azuresqldb_engine()

  # Load all data
  insert_tables(data, engine)
  
  # Status Update
  print('Tables loaded successfully to database!'

# Conditional Execution
if __name__ == "__main__":
  load()
