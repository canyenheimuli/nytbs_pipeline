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
    df['list_date'] = pd.to_datetime(df['pub_date']) - pd.Timedelta(days = 11) # The list always comes out 11 days before it's published in print
    
    # Extract year/month/day
    df['list_date_year'] = df['list_date'].dt.year
    df['list_date_month'] = df['list_date'].dt.month
    
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
    'list_id', 'rank', 'list_date'
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
        'list_date',
        'list_date_year',
        'list_date_month',
        'retrieval_date'
    ]
    
    df = df[req_cols]
    
    # Re-naming
    df = df.rename(columns = {
        'primary_isbn13': 'isbn13',
        'rank': 'book_rank',
        'rank_last_week': 'rank_last_period',
        'weeks_on_list': 'periods_on_list',
        'description': 'book_descr'
    })
    
    # Output
    return df

# Normalizes all data in prep for load
def normalize(df: pd.DataFrame) -> dict:
    '''
    Takes the single cleaned results table
    and normalizes according to DB setup
    '''
    
    # Weekly and Monthly Tables
    lists_cols = [
        'list_id',	
        'book_rank',
        'isbn13',
        'rank_last_period',	
        'periods_on_list', 
        'list_date', 	
        'list_date_year',	
        'list_date_month',
        'retrieval_date'
    ]
    
    weekly_lists = df.loc[df['update_freq'] == 'WEEKLY', lists_cols]
    monthly_lists = df.loc[df['update_freq'] == 'MONTHLY', lists_cols]
    
    # List Names
    list_info = df[['list_id', 'list_name']].drop_duplicates()
    
    # Book Info
    books_info_cols = [
        'isbn13',	
        'title',
        'author',
        'book_descr',	
        'publisher', 
        'book_image'
    ]
    
    books = df[books_info_cols].drop_duplicates(subset = 'isbn13')
    
    # Output
    return {
        'weekly_lists': weekly_lists,
        'monthly_lists': monthly_lists,
        'list_info': list_info,
        'books': books
    }

# Consolidated Transformer Fn
def transform(df: pd.DataFrame) -> dict:
    '''
    Performs the following transformations:
    - Converts certain cols to numbers
    - Splits out the date column
    - Titleize the title column
    - Remove any duplicates of key vars
    - Cleans up column names and orders
    - Normalizes data into tables for loading
    '''
    df = split_time_vars(df)
    df = split_time_vars(df)
    df = format_titles(df)
    df = remove_duplicates(df)
    df = cols_cleaning(df)
    df_dict = normalize(df)
    
    return df_dict

# Conditional Execution
if __name__ == "__main__":
    transform()