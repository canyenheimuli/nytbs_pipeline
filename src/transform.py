# Packages
import pandas as pd

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
  df['updated_date_day'] = df['updated_date'].dt.day

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
    'updated_date',
    'retrieval_date'
  ]

  df = df[req_cols]

  # Re-naming
  df = df.rename(columns = {'primary_isbn13': 'isbn13'})

  # Output
  return df

# Remove any duplicates of key vars fn
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Key vars implied by this step are
  'list_id', 'rank', 'updated_date'
  '''
  return df.drop_duplicates(subset = ['list_id', 'rank', 'updated_date'], keep = 'first')

# Consolidated Transformer Fn
def transform(df: pd.DataFrame) -> pd.DataFrame:
  '''
  Performs the following transformations:
  - Splits out the date column
  - Titleize the title column
  - Cleans up columns
  - Remove any duplicates of key vars
  '''
  df = split_time_vars(df)
  df = format_titles(df)
  df = cols_cleaning(df)
  df = remove_duplicates(df)

  return df

# Conditional Execution
if __name__ == "__main__":
  transform()
