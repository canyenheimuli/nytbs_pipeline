# Packages
import pandas as pd

# Re-order and Split Queried List Data fn.
def process_list_df(lists_df: pd.DataFrame, lists_freq: str) -> list[pd.DataFrame]:
    '''
    Re-order and split a weekly or monthly 
    full DF of period-specific lists
    '''
    # Set List ID Order
    if lists_freq == "weekly":
        list_ids_order = [704, 708, 1, 2, 17, 24, 10, 14, 13, 7] # Lists are ordered subjectively based on list popularity and intended age ranges
    elif lists_freq == "monthly":
        list_ids_order = [10018, 301, 302, 10004, 532, 304, 719, 10016, 10015, 303] # Lists are ordered subjectively based on list popularity and intended age ranges

    # Make new var, arrange
    lists_df['list_id_sorted'] = pd.Categorical(
        lists_df['list_id'], 
        categories=list_ids_order, 
        ordered=True
    )
    lists_df = lists_df.sort_values(by=['list_id_sorted', 'rank'])
    
    return [group for _, group in lists_df.groupby('list_id_sorted', observed=True)]
