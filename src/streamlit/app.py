# %% Packages
import streamlit as st
import viz_queries as vq
import app_utils
from datetime import date, datetime
import pandas as pd

# %% Back-end data, other objects
# Cached Data Queries
weekly_lists_latest = vq.query_latest_weeklies()
monthly_lists_latest = vq.query_latest_weeklies()

avail_weeks = vq.get_avail_weeks()
avail_months = vq.get_avail_months()

# Viz Params
current_year = datetime.now().year
years = list(range(2008, datetime.now().year + 1))
months = [
    "January", "February", "March", "April", "May", "June", 
    "July", "August", "September", "October", "November", "December"
]

cols_per_row = 5

# %%% UI
# %% Landing Page Info
# Landing Page UI
st.set_page_config(page_title = "NYT BS Lists Dashboard", layout = "wide")
st.title("NYT Bestseller Lists Historical Viewer")
st.markdown("Use the tabs below to navigate through different pages.")

# %% Tabs
# Tabs
tab_1, tab_2, tab_3 = st.tabs(["Overview", "Weekly Bestseller Lists", "Monthly Bestsller Lists"])

# %% Session States Dict
# Make One State per Tab
if "tab_state" not in st.session_state:
    st.session_state.tab_state = {
        tab: {
            "view_mode": "latest",
            "active_period": None,
            "filtered_df": None,
        }
        for tab in ["tab_2", "tab_3"]
    }

# %% Overview Tab
with tab_1:
    st.title("Overview")
    st.write("This dashboard shows the current and historical NYT Bestseller Lists.")
    st.write("Click on the \"Weekly Lists\" or \"Monthly Lists\" tab to see the current lists.")
    st.write("Once in a tab view, use the date filters to look up past lists and rankings.")

# %% Weeklies Tab
with tab_2:
    
    # Get Session State for Tab
    state = st.session_state.tab_state["tab_2"]
    
    # Tab Title
    st.title("Weekly Lists")

    # Filter Controls
    selected_date = st.date_input(
        "Filter by date",
        value=date.today(),
        key="tab_2_date_input"
    )

    nearest_week = vq.find_nearest_week(selected_date, avail_weeks)
    st.caption(f"Nearest available week: **{nearest_week}**")

    col1, col2 = st.columns(2)
    apply = col1.button("Apply filter", key="tab_2_apply")
    reset = col2.button("Reset to latest", key="tab_2_reset")

    # Button logic
    if apply:
        state["view_mode"]      = "filtered"
        state["active_period"]  = nearest_week
        state["filtered_df"]    = vq.query_filtered_weeklies(nearest_week)

    if reset:
        state["view_mode"]      = "latest"
        state["active_period"]  = None
        state["filtered_df"]    = None

    # Get Data Based on State
    if state["view_mode"] == "latest":
        st.subheader(f"Latest data — week of {avail_weeks[0]}")
        curr_data = weekly_lists_latest
    else:
        st.subheader(f"Filtered data — week of {state['active_period']}")
        curr_data = state["filtered_df"]

    curr_data_split = app_utils.process_list_df(curr_data, "weekly")

    # Loop through weekly lists in df vector
    for weekly_list in curr_data_split:
    
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
                                st.image(r_image, width=200)
                            else:
                                st.caption("No image available")
                            
                            # Fallbacks for individual text elements
                            disp_rank   = r_rank if r_rank != "" else "N/A"
                            disp_title  = r_title if r_title != "" else "Unknown Title"
                            disp_author = r_author if r_author != "" else "Unknown Author"
    
                            # Book info
                            st.markdown(f"\\#{disp_rank}: {disp_title} by {disp_author}")
                        
# %% Monthlies Tab
with tab_3:
    
    # Get Session State for Tab
    state = st.session_state.tab_state["tab_3"]
    
    # Tab Title
    st.title("Monthly Lists")

    # Month Input Boxes
    col_m, col_y = st.columns(2)
    
    with col_m:
        selected_month = st.selectbox("Select Month", months, index=datetime.now().month - 1)
    
    with col2:
        selected_year = st.selectbox("Select Year", years, index=years.index(current_year))
    
    selected_month_number = months.index(selected_month) + 1
    selected_date = datetime(selected_year, selected_month_number, 1)
    st.caption(f"Selected month: **{selected_month} {selected_year}**")

    # Filter Buttons
    col_filter, col_reset = st.columns(2)
    apply = col_filter.button("Apply filter", key="tab_3_apply")
    reset = col_reset.button("Reset to latest", key="tab_3_reset")

    # Button logic
    if apply:
        state["view_mode"]      = "filtered"
        state["active_period"]  = selected_date
        state["filtered_df"]    = vq.query_filtered_monthlies(selected_date)

    if reset:
        state["view_mode"]      = "latest"
        state["active_period"]  = None
        state["filtered_df"]    = None

    # Get Data Based on State
    if state["view_mode"] == "latest":
        st.subheader(f"Latest data — month of {avail_months[0]}")
        curr_data = weekly_lists_latest
    else:
        st.subheader(f"Filtered data — month of {selected_month} {selected_year}")
        curr_data = state["filtered_df"]

    curr_data_split = app_utils.process_list_df(curr_data, "monthly")
    
    # Tab Title
    st.title("Monthly Lists")
    
    # Loop through weekly lists in df vector
    for monthly_list in curr_data_split:

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
                                st.image(r_image, width=200)
                            else:
                                st.caption("No image available")
                            
                            # Fallbacks for individual text elements
                            disp_rank   = r_rank if r_rank != "" else "N/A"
                            disp_title  = r_title if r_title != "" else "Unknown Title"
                            disp_author = r_author if r_author != "" else "Unknown Author"
    
                            # Book info
                            st.markdown(f"\\#{disp_rank}: {disp_title} by {disp_author}")
