# Packages
from datetime import datetime, date, timedelta
import pandas as pd

# Benchmark user-supplied date to nearest DB week
def find_nearest_week(selected_date: date, available_weeks: list[date]) -> date:
    '''
    Takes a user-supplied date and benchmarks 
    it to an existing week of pre-prepared available weeks
    '''
    past_weeks = [w for w in available_weeks if w <= selected_date]
    return max(past_weeks) if past_weeks else None

# Get code for current cycle (week)
def get_cycle_code() -> str:
    '''
    Gets string for the current (or last)
    week's Thursday to use in cache cycle
    '''
    # Get date, benchmark to most recent Thursday, and use as a cache key for weeklies
    today = datetime.now().date()
    days_since_thu = (today.weekday() - 3) % 7
    most_recent_thu = today - timedelta(days=days_since_thu)
    return most_recent_thu.strftime("%Y-%m-%d")

# Re-order and Split Queried List Data fn.
def process_list_df(lists_df: pd.DataFrame, lists_freq: str) -> list[pd.DataFrame]:
    '''
    Re-order and split a weekly or monthly 
    full DF of period-specific lists
    '''
    # Make copy of input
    output_df = lists_df.copy()
    
    # Set List ID Order
    if lists_freq == "weekly":
        list_ids_order = [704, 708, 1, 2, 17, 24, 10, 14, 13, 7] # Lists are ordered subjectively based on list popularity and intended age ranges
    elif lists_freq == "monthly":
        list_ids_order = [10018, 301, 302, 10004, 532, 304, 719, 10016, 10015, 303] # Lists are ordered subjectively based on list popularity and intended age ranges

    # Make new var, arrange
    output_df['list_id_sorted'] = pd.Categorical(
        output_df['list_id'], 
        categories=list_ids_order, 
        ordered=True
    )
    output_df = output_df.sort_values(by=['list_id_sorted', 'rank'])
    
    return [group for _, group in output_df.groupby('list_id_sorted', observed=True)]

# TO-DO: Get all list names from the longest runners table
def collect_list_names_from_longest_runners(longest_runners: pd.DataFrame) -> list[str]:
    '''
    Extract all list names from the
    "longest runners" query table (NOTE: there may be a more efficient way to get these)
    '''
    longest_runners['list_name'].drop_duplicates().sort_values()

# Process longest runners df fn.
def process_longest_runners_df(longest_runners: pd.DataFrame, list_name: str = None, top_n_books: int = 10) -> pd.DataFrame:
    '''
    Filter (or don't) to longest runners to 
    input list name and get top n of them
    '''
    # Make copy of input
    output_df = longest_runners.copy()

    # Conditional Filter
    if list_name is not None:
        output_df = output_df[output_df['list_name'] = list_name]

    # Output
    return output_df.head(top_n_books)
