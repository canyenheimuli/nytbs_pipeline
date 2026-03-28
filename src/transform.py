# Packages
import pandas as pd

# Check numeric cols
def convert_numeric_cols(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Convert key cols to numeric.
  If any values are non-numeric, replace them with NaN, so they can be removed downstream in the data transformations.
  '''
  df['primary_isbn13'] = pd.to_numeric(df['primary_isbn13'], errors = 'coerce') # It's possible for the NYT to publish faulty ISBN-13 values leading to bugs; current procedure is to just drop them
  df['rank'] = pd.to_numeric(df['rank'], errors = 'coerce')
  df['rank_last_week'] = pd.to_numeric(df['rank_last_week'], errors = 'coerce')
  df['list_id'] = pd.to_numeric(df['list_id'], errors = 'coerce')
  
  return df

# Split datetime vars fn
def split_time_vars(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Splits the input df's 'updated_date' var
  into separate columns for aggregation
  '''
  # Convert to datetime
  df['updated_date'] = pd.to_datetime(df['updated_date'])

  # Extract year/month/day
  df['updated_date_year'] = df['updated_date'].dt.year
  df['updated_date_month'] = df['updated_date'].dt.month

  # Output
  return df

# Titleize title column fn
def format_titles(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Formats the 'title' column from
  all caps to title format with first
  letters of each word capitalized
  '''
  df['title'] = df['title'].str.title()
  return df

# Remove any duplicates of key vars fn
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Key vars implied by this step are
  'list_id', 'rank', 'updated_date'
  '''
  return df.drop_duplicates(subset = ['list_id', 'rank', 'updated_date'], keep = 'first')

# Filter to required columns fn
def cols_cleaning(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Filters input data down to
  only the required columns and
  renames and re-orders columns
  '''
  # Column subsetting and re-ordering
  req_cols = [
    'list_name',
    'list_id',	
    'update_freq',
    'rank',
    'title',	
    'author', 
    'description', 	
    'primary_isbn13',	
    'publisher',
    'rank_last_week',
    'weeks_on_list', 
    'book_image',
    'updated_date',
    'updated_date_year',
    'updated_date_month',
    'retrieval_date'
  ]

  df = df[req_cols]

  # Re-naming
  df = df.rename(columns = {
    'primary_isbn13': 'isbn13',
    'rank': 'book_rank',
    'rank_last_week': 'rank_last_period',
    'weeks_on_list': 'periods_on_list',
    'updated_date': 'pub_date',
    'updated_date_year': 'pub_date_year',
    'updated_date_month': 'pub_date_month',
    'description': 'book_descr'
  })

  # Output
  return df

# Consolidated Transformer Fn
def transform(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Performs the following transformations:
  - Converts certain cols to numbers
  - Splits out the date column
  - Titleize the title column
  - Remove any duplicates of key vars
  - Cleans up column names and orders
  '''
  df = split_time_vars(df)
  df = split_time_vars(df)
  df = format_titles(df)
  df = remove_duplicates(df)
  df = cols_cleaning(df)

  return df

# Conditional Execution
if __name__ == "__main__":
  transform()
