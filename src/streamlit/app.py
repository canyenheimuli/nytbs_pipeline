# Packages
import streamlit as st
import viz_db as vdb

# Back-end (data queries)
weekly_lists = vdb.query_latest_weeklies(list_id=704) # TO-DO: Update with dynamic list ID retrieval
monthly_lists = vdb.query_latest_monthlies(list_id=532) # TO-DO: Update with dynamic list ID retrieval

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
    st.write("Weekly lists currently include \"Combined Fiction\", and monthly lists currently include \"Business and Self-help\", with more lists to be added soon.")

# Weeklies Tab
with tab_2:
    # Tab Title
    st.title("Weekly Lists")
    
    # Combined List Section
    st.header("Combined Print/E-Book Fiction")
    
    # Break up list into chunks with length 5
    for i in range(0, len(weekly_lists), cols_per_row):
        # Subset data for viz row
        weeklies_row = weekly_lists[i:i + cols_per_row]
        
        # Create the row layout dynamically
        r_cols = st.columns(cols_per_row)
        
        # Loop parallel over inputs into st_cols
        for r_rank, r_title, r_author, r_image, r_col in zip(weeklies_row['rank'], weeklies_row['title'], weeklies_row['author'], weeklies_row['image'], r_cols):
            with r_col:
                st.image(r_image, width=100)
                st.markdown(f"{r_title} by {r_author} ({r_rank})")

# Monthlies Tab
with tab_3:
    # Tab Title
    st.title("Monthly Lists")
    
    # Business Section
    st.header("Business")
    
    # Break up list into chunks with length 5
    for i in range(0, len(monthly_lists), cols_per_row):
        # Subset data for viz row
        monthlies_row = monthly_lists[i:i + cols_per_row]
        
        # Create the row layout dynamically
        r_cols = st.columns(cols_per_row)
        
        # Loop parallel over inputs into st_cols
        for r_rank, r_title, r_author, r_image, r_col in zip(monthlies_row['rank'], monthlies_row['title'], monthlies_row['author'], monthlies_row['image'], r_cols):
            with r_col:
                st.image(r_image, width=100)
                st.markdown(f"{r_title} by {r_author} ({r_rank})")
