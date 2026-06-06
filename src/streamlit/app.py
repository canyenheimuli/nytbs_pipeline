# Packages
import streamlit as st
import viz_db as vdb
import pandas as pd

# Back-end (data queries)
weekly_lists = vdb.query_weeklies()
monthly_lists = vdb.query_monthlies()

# Arrange weeklies
w_lists_order = [704, 708, 1, 2, 17, 24, 10, 14, 13, 7] # Lists are ordered subjectively based on list popularity and intended age ranges

weekly_lists['list_id_sorted'] = pd.Categorical(
    weekly_lists['list_id'], 
    categories=w_lists_order, 
    ordered=True
)
weekly_lists = weekly_lists.sort_values(by=['list_id_sorted', 'rank'])

weeklies_split = [group for _, group in weekly_lists.groupby('list_id_sorted', observed=True)]

# Arrange monthlies
m_lists_order = [10018, 301, 302, 10004, 532, 304, 719, 10016, 10015, 303] # Lists are ordered subjectively based on list popularity and intended age ranges

monthly_lists['list_id_sorted'] = pd.Categorical(
    monthly_lists['list_id'], 
    categories=m_lists_order, 
    ordered=True
)
monthly_lists = monthly_lists.sort_values(by=['list_id_sorted', 'rank'])

monthlies_split = [group for _, group in monthly_lists.groupby('list_id_sorted', observed=True)]

# Viz Params
cols_per_row = 5

# Landing Page UI
st.set_page_config(page_title = "NYT BS Lists Dashboard", layout = "wide")
st.title("NYT Bestseller Lists Historical Viewer")
st.markdown("Use the tabs below to navigate through different pages.")

## Set Tabs
tab_1, tab_2, tab_3 = st.tabs(["Overview", "Weekly Bestseller Lists", "Monthly Bestsller Lists"])

# Overview Tab
with tab_1:
    st.title("Overview")
    st.write("This dashboard shows the current and historical NYT Bestseller Lists.")
    st.write("Click on the \"Weekly Lists\" or \"Monthly Lists\" tab to see the current lists.")

# Weeklies Tab
with tab_2:
    # Tab Title
    st.title("Weekly Lists")

    # Loop through weekly lists in df vector
    for weekly_list in weeklies_split:
    
        # Containerize (card widget-ize) each list
        with st.container():
            # List title
            list_name = weekly_list["list_name"].values[0]
            st.header(list_name)
            
            # Break up list into chunks with length 5
            for i in range(0, len(weekly_list), cols_per_row):
                # Subset data for viz row
                weeklies_row = weekly_list[i:i + cols_per_row]
                
                # Create the row layout dynamically
                r_cols = st.columns(cols_per_row)
                
                # Loop parallel over inputs into st_cols
                for r_rank, r_title, r_author, r_image, r_col in zip(weeklies_row['rank'], weeklies_row['title'], weeklies_row['author'], weeklies_row['image'], r_cols):
                    with r_col:
                        # Check for essential data
                        if r_title == "" and r_author == "" and r_image == "":
                            # Control flow for empty ranking data
                            st.caption("Rank empty")
                        else:
                            # Render image if it exists
                            if r_image:
                                st.image(r_image, width=150)
                            else:
                                st.caption("No image available")
                            
                            # Fallbacks for individual text elements
                            disp_rank = r_rank if r_rank != "" else "N/A"
                            disp_title = r_title if r_title != "" else "Unknown Title"
                            disp_author = r_author if r_author != "" else "Unknown Author"
    
                            # Book info
                            st.markdown(f"\\#{disp_rank}: {disp_title} by {disp_author}")
                        
# Monthlies Tab
with tab_3:
    # Tab Title
    st.title("Monthly Lists")
    
    # Loop through weekly lists in df vector
    for monthly_list in monthlies_split:

        # Containerize (card widget-ize) each list
        with st.container():
            # List Title
            list_name = monthly_list["list_name"].values[0]
            st.header(list_name)
        
            # Break up list into chunks with length 5
            for i in range(0, len(monthly_list), cols_per_row):
                # Subset data for viz row
                monthlies_row = monthly_list[i:i + cols_per_row]
                
                # Create the row layout dynamically
                r_cols = st.columns(cols_per_row)
                
                # Loop parallel over inputs into st_cols
                for r_rank, r_title, r_author, r_image, r_col in zip(monthlies_row['rank'], monthlies_row['title'], monthlies_row['author'], monthlies_row['image'], r_cols):
                    with r_col:
                        # Check for essential data
                        if r_title == "" and r_author == "" and r_image == "":
                            # Control flow for empty ranking data
                            st.caption("Rank empty")
                        else:
                            # Render image if it exists
                            if r_image:
                                st.image(r_image, width=150)
                            else:
                                st.caption("No image available")
                            
                            # Fallbacks for individual text elements
                            disp_rank = r_rank if r_rank != "" else "N/A"
                            disp_title = r_title if r_title != "" else "Unknown Title"
                            disp_author = r_author if r_author != "" else "Unknown Author"
    
                            # Book info
                            st.markdown(f"\\#{disp_rank}: {disp_title} by {disp_author}")
