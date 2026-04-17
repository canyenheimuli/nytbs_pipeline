# Packages
from datetime import datetime
import numpy as np
import pandas as pd

# All required cols
def check_req_cols(df: pd.DataFrame) -> None:
    '''
    Check whether the DataFrame content contains the requred columns from the NYT Bestsellers list. 
    Otherwise, raise an error.
    Add essential cols for dataviz to THIS step as necessary.
    '''
    req_cols = [
      'author', 
      'description', 	
      'primary_isbn13',	
      'publisher',
      'rank',
      'rank_last_week',
      'title',	
      'pub_date',
      'weeks_on_list', 
      'list_name',
      'list_id',	
      'update_freq',
      'retrieval_date'
    ]
    
    if not set(req_cols).issubset(df.columns):
        raise ValueError('Data does not include all of the required fields.')

# Verify date time format for all date cols
def verify_date_cols(df: pd.DataFrame) -> None:
    '''
    Verify all date cols follow ISO 8601 format.
    Raise a ValueError if any of the 'date_time' values are invalid.
    '''
    df.apply(lambda row: datetime.fromisoformat(str(row['pub_date'])), axis = 1) 
    df.apply(lambda row: datetime.fromisoformat(str(row['retrieval_date'])), axis = 1) 

# Checks for missing values among key columns
def check_missing_vals(df: pd.DataFrame) -> None:
    '''
    Check whether there are any missing values in key columns.
    (e.g., same as required columns in the  excluding 'description')
    '''  
    req_data_cols = [
      'author', 
      'primary_isbn13',	
      'publisher',
      'rank',
      'rank_last_week',
      'title',	
      'pub_date',
      'weeks_on_list', 
      'list_name',
      'list_id',	
      'update_freq',
      'retrieval_date'
    ]
    
    null_cols = df[req_data_cols].columns[df[req_data_cols].isnull().any()].tolist()
    
    if null_cols:
        raise ValueError(f"Missing values are present in the following attributes: {', '.join(null_cols)}")

# Check that the dataframe has the expected rank format
def check_ranks(df: pd.DataFrame, sort_col: "str" = "rank") -> None:
    """
    Ensures that the input data follows the expected ranking structure
    (expected structure is dense ranks within each list_id grouping);
    Raises an error otherwise
    """
    # Get dense rankings based on sort col ("rank")
    df["rank_check"] = df.groupby("list_id")[sort_col] \
        .rank(method = "dense") \
        .astype(int)

    # Ensure they match
    rank_check = (df["rank_check"] == df[sort_col]).all()
    
    if not rank_check:
        raise ValueError("Data column 'rank' does not follow expected structure.")

# Consolidated Validator Fn
def validate(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Check the data satisfies the following data quality checks:
    - Has the right cols
    - Dates follows ISO 8601 format
    - No missing values in columns that require data
    - Rank cols follow the expected format of ranks
    '''
    check_req_cols(df)
    verify_date_cols(df)
    check_missing_vals(df)
    check_ranks(df)
    
    return df

# Conditional Execution
if __name__ == "__main__":
    validate()
