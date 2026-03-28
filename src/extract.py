# Packages
import requests
import json
import pandas as pd
from datetime import date
import os
from dotenv import load_dotenv

# Load Env
load_dotenv()

# Constants
API_KEY = os.environ.get("NYTBOOKS_API_KEY")
OVERVIEW_URL = "https://api.nytimes.com/svc/books/v3/lists/overview.json"

# Fast-fail if key is missing
if not API_KEY:
    raise EnvironmentError("NYTBOOKS_API_KEY is not set. Check your .env file.")

# Get results overview fn
def get_nytbs_overview(url: str, params: dict | None = None) -> dict:
  '''
  Get the rankings from all lists for a given period
  and return as a python dictionary
  '''
  # Params fallback
  if params is None:
    params = {"api-key": API_KEY}
  
  # Try to get overview with exceptions
  try:
    # Get response
    response = requests.get(url = url, params = params, timeout = 10)

    # Raise an exception for bad status codes (4xx or 5xx)
    response.raise_for_status()

    # Parse the JSON response into a Python dictionary, return
    return(response.json())

  # HTTP except
  except requests.exceptions.HTTPError as e:
    print(f"HTTP error occurred: {e}")

  # API Call except
  except requests.exceptions.RequestException as e:
    print(f"An error occurred during the API call: {e}")

  # Key except
  except json.JSONDecodeError as e:
      raise RuntimeError(f"Failed to parse JSON response: {e}") from e

# Clean List Dict fn
def process_list(book_list, overview) -> pd.DataFrame:
  '''
  Turns the book list dict into an individual
  pd DataFrame including some overview metadata
  '''
  return pd.DataFrame(book_list['books']).assign(
    list_name = book_list['list_name'],
    list_id = book_list['list_id'],
    update_freq = book_list['updated'],
    updated_date = overview['results']['bestsellers_date'],
    retrieval_date = date.today()
  )

# Full extract fn
def extract(url: str = OVERVIEW_URL) -> pd.DataFrame:

  # Get overview
  overview = get_nytbs_overview(url = url)

  # Process and combine results
  dfs_list = [process_list(book_list, overview) for book_list in overview['results']['lists']]
  return pd.concat(dfs_list, ignore_index = True)

# Conditional for module importing
if __name__ == "__main__":
  extract()
