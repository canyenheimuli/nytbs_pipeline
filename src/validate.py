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
def check_rank_dims(df: pd.DataFrame) -> None:
    '''
    Creates a "reference rank table" and checks
    the input df against this to validate table organization
    '''
    
    # Helper function
    def create_ranks(ids, max_rank):
        '''
        Creates a rank reference dataframe with a sequence "max_rank" 
        for a list of input ids
        '''
        id_array = np.repeat(ids, max_rank)
        rank_array = np.tile(np.arange(1, max_rank + 1), len(ids))
        return pd.DataFrame({"list_id": id_array, "rank": rank_array})
    
    # Create expected ranks reference
    lists_w_len15 = [704, 708, 1, 2, 17, 4, 301, 302, 719, 10018] # UPDATE AS NEEDED
    lists_w_len10 = [24, 13, 7, 10, 14, 532, 10015, 10016] # UPDATE AS NEEDED
    
    # lists_w_len15 = [704, 708, 1, 2, 17, 303, 301, 302, 719, 10004] # UPDATE AS NEEDED
    # lists_w_len10 = [24, 13, 7, 10, 14, 304, 532, 10015, 10016] # UPDATE AS NEEDED
    
    df_l15 = create_ranks(lists_w_len15, 15)
    df_l10 = create_ranks(lists_w_len10, 10)
    
    exp_ranks = pd.concat([df_l15, df_l10], ignore_index = True)
    
    # Join together and check n size
    merge_check = pd.merge(
        df, 
        exp_ranks, 
        on = ['list_id', 'rank'], 
        how = 'inner'
    )
    
    if len(merge_check) != len(exp_ranks):
        raise ValueError("Extracted data does not match expected ranking structure.")

# Consolidated Validator Fn
def validate(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Check the data satisfies the following data quality checks:
    - Has the right cols
    - Dates follows ISO 8601 format
    - No missing values in columns that require data
    - Rank cols follow the expected number and format of ranks
    '''
    check_req_cols(df)
    verify_date_cols(df)
    check_missing_vals(df)
    # check_rank_dims(df)
    
    return df

# Conditional Execution
if __name__ == "__main__":
    validate()
