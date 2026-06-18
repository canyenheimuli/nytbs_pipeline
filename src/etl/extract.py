# Packages
import requests
import json
import time
import pandas as pd
from datetime import date
import os
from dotenv import load_dotenv

# Load Env
load_dotenv()

# Constants
NYT_API_KEY = os.environ.get("NYT_API_KEY")
NYT_BSLISTS_OVERVIEW_URL = "https://api.nytimes.com/svc/books/v3/lists/overview.json"

# Fast-fail if key is missing
if not NYT_API_KEY:
    raise EnvironmentError("NYT API Key is not set. Check your .env file.")

# Get results overview fn
def get_nytbs_overview(url: str, params: dict | None = None, date: str | None = None, err_500_attempts: int = 3, err_500_retry_wait: int = 60) -> dict:
    '''
    Get the rankings from all lists for a given period
    and return as a python dictionary
    '''
    # Params fallback
    if params is None:
        # Set API Key
        params = {"api-key": NYT_API_KEY}
        
        # Conditionally set date
        if date is not None:
            params["published_date"] = date
    
    # Try to get overview with exceptions
    while err_500_attempts > 0:
        # Attempt request
        try:
            # Get response
            response = requests.get(url = url, params = params, timeout = 10)
            
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()
            
            # Parse the JSON response into a Python dictionary, return
            return(response.json())
        
        # HTTP except
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                # Status Code 500 control flow
                err_500_attempts -= 1
                print(f"Received Status Code 500; retrying in {err_500_retry_wait} seconds")
                time.sleep(err_500_retry_wait)
                continue
            else:
                # Other
                print(f"HTTP error occurred: {e}")
                break
        
        # API Call except
        except requests.exceptions.RequestException as e:
            print(f"An error occurred during the API call: {e}")
            break
        
        # Key except
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse JSON response: {e}") from e
            break

# Clean List Dict fn
def process_list(book_list: dict, overview: dict) -> pd.DataFrame:
    '''
    Turns the book list dict into an individual
    pd DataFrame including some overview metadata
    '''
    return pd.DataFrame(book_list['books']).assign(
        list_name = book_list['list_name'],
        list_id = book_list['list_id'],
        update_freq = book_list['updated'],
        pub_date = overview['results']['published_date'],
        retrieval_date = date.today()
    )

# Full extract fn
def extract(url: str = NYT_BSLISTS_OVERVIEW_URL, date: str | None = None) -> pd.DataFrame:
    '''
    Gets the overview JSON from the API and
    processes the list into a pandas dataframe
    '''
    # Get overview
    overview = get_nytbs_overview(url = url, date = date)
    
    # Process and combine results
    dfs_list = [process_list(book_list, overview) for book_list in overview['results']['lists']]
    return pd.concat(dfs_list, ignore_index = True)

# Conditional for module importing
if __name__ == "__main__":
    extract()
