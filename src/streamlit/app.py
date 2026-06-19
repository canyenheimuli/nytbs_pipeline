# %% Packages
import streamlit as st
from datetime import date, datetime
import pandas as pd

import viz_queries as vq
import app_utils

# %% Back-end data, other objects
# Loading message spinner
with st.spinner("Fetching NYT Bestseller Lists data ⏳"):
    # Cached data
    weekly_lists_latest  = vq.query_latest_weeklies(week_cycle_code = app_utils.get_cycle_code())
    weekly_lists_hist    = vq.query_hist_weeklies(week_cycle_code = app_utils.get_cycle_code())
    avail_weeks          = vq.get_avail_weeks(week_cycle_code = app_utils.get_cycle_code())
    monthly_lists_latest = vq.query_latest_monthlies(month_str = datetime.now().strftime("%Y-%m"))
    monthly_lists_hist   = vq.query_hist_monthlies(month_str = datetime.now().strftime("%Y-%m"))
    avail_months         = vq.get_avail_months(month_str = datetime.now().strftime("%Y-%m"))

# Viz Params
years = list(range(2008, datetime.now().year + 1))
current_year = datetime.now().year

months = [
    "January", "February", "March", "April", "May", "June", 
    "July", "August", "September", "October", "November", "December"
]
current_month = months[datetime.now().month - 1]

cols_per_row = 5

# %%% UI
# %% Landing Page Info
# Landing Page UI
st.set_page_config(page_title = "NYT BS Lists Dashboard", layout = "wide")
st.title("NYT Bestseller Lists Historical Viewer", anchor=False)

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
    st.markdown(f"""
    The NYT Bestseller Lists Historical Viewer is a multi-tab dashboard view of the current and historical NYT Bestsellers book lists.

    To use the historical viewer, click one of the list tabs at the top of this page. Input a date in the "Select a date" input boxes, then press \
    "Apply filter" to view the lists for that date. To remove a selected filter and view the latest lists, push the "Reset to latest" button.

    Currently, the app can retrieve and show list data ranging from **{avail_weeks[-1].strftime("%B %d, %Y")}** to **{avail_weeks[0].strftime("%B %d, %Y")}**, the most recent list.

    ## Background
    The NYT Bestseller Lists are rankings of the most popular books in the United States ordered by recent sales. The lists are prepared using the \
    NYT's proprietary ranking methodology based on sales volume, variety of points of sale, and other data; and are updated regularly (weekly or \
    monthly, depending on list type).

    Weekly lists are the paper's flagship bestseller book rankings. Weekly lists include the following and several more:
    - Combined Print & E-book Fiction
    - Combined Nonfiction
    - Advice Books
    - Young Adult Series

    These lists are updated every Wednesday at approximately 7pm Eastern, and each updated list is then published 11 days later in that week's edition \
    of the New York Times Book Review. 

    Monthly lists include books of unique formats or audiences, such as the following:
    - Audio Fiction & Nonfiction
    - Business
    - Graphic Novels
    - Paperback YA, Middle Grade, and Children's books

    These lists are typically updated on the first Wednesday of every month and at the same time as the weekly lists.
    
    In this app's internal database, lists are denoted by the date they were updated by the NYT -- _not_ the date the lists were subsequently published in the paper.

    For more information on the Bestseller lists, please see the NYT's [methodology page](https://www.nytimes.com/books/best-sellers/methodology/).
    """)

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
        min_value=avail_weeks[-1],
        max_value=date.today(),
        key="tab_2_date_input"
    )

    nearest_week = app_utils.find_nearest_week(selected_date, avail_weeks)
    st.caption(f"Available date range: **{avail_weeks[-1].strftime("%B %d, %Y")} to {date.today().strftime("%B %d, %Y")}**")
    st.caption(f"Selected date: **{selected_date.strftime("%B %d, %Y")}**")
    st.caption(f"Nearest available week: **{nearest_week.strftime("%B %d, %Y")}**")

    col1, col2 = st.columns(2)
    apply = col1.button("Apply filter", key="tab_2_apply")
    reset = col2.button("Reset to latest", key="tab_2_reset")

    # Button logic
    if apply:
        state["view_mode"]     = "filtered"
        state["active_period"] = nearest_week
        state["filtered_df"]   = weekly_lists_hist.loc[lambda x: x['list_date'] == nearest_week]

    if reset or (apply and nearest_week == app_utils.find_nearest_week(date.today(), avail_weeks)):
        state["view_mode"]     = "latest"
        state["active_period"] = None
        state["filtered_df"]   = None

    # Get Data Based on State
    if state["view_mode"] == "latest":
        st.subheader(f"Latest data — week of {avail_weeks[0].strftime("%B %d, %Y")}")
        curr_data = weekly_lists_latest
    else:
        st.subheader(f"Filtered data — week of {state['active_period'].strftime("%B %d, %Y")}")
        curr_data = state["filtered_df"]

    # Error caption if data is empty
    if curr_data.empty:
        st.caption("No data available for this time period")

    # Show data if exists
    else:
        # Split data by group for viz
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
                                    st.image(r_image, width=175)
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
    
    with col_y:
        selected_year = st.selectbox("Select Year", years, index=years.index(current_year))
    
    selected_month_number = months.index(selected_month) + 1
    st.caption(f"Available date range: **{avail_weeks[-1].strftime("%B, %Y")} to {datetime.now().strftime("%B, %Y")}**")
    st.caption(f"Selected month: **{selected_month} {selected_year}**")
    
    # Filter Buttons
    col_filter, col_reset = st.columns(2)
    apply = col_filter.button("Apply filter", key="tab_3_apply")
    reset = col_reset.button("Reset to latest", key="tab_3_reset")

    # Button logic
    if apply:
        state["view_mode"]     = "filtered"
        state["active_period"] = datetime(selected_year, selected_month_number, 1)
        state["filtered_df"]   = monthly_lists_hist.loc[lambda x: (x['list_date_year'] == selected_year) & (x['list_date_month'] == selected_month_number)]

    if reset or (apply and selected_year == current_year and selected_month == current_month):
        state["view_mode"]     = "latest"
        state["active_period"] = None
        state["filtered_df"]   = None

    # Get Data Based on State
    if state["view_mode"] == "latest":
        st.subheader(f"Latest data — month of {date.today().strftime("%B, %Y")}")
        curr_data = monthly_lists_latest
    else:
        st.subheader(f"Filtered data — month of {state['active_period'].strftime("%B, %Y")}")
        curr_data = state["filtered_df"]

    # Error caption if data is empty
    if curr_data.empty:
        st.caption("No data available for this time period")

    # Show data if exists
    else:
        # Split data by group for viz
        curr_data_split = app_utils.process_list_df(curr_data, "monthly")
        
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
                                    st.image(r_image, width=175)
                                else:
                                    st.caption("No image available")
                                
                                # Fallbacks for individual text elements
                                disp_rank   = r_rank if r_rank != "" else "N/A"
                                disp_title  = r_title if r_title != "" else "Unknown Title"
                                disp_author = r_author if r_author != "" else "Unknown Author"
        
                                # Book info
                                st.markdown(f"\\#{disp_rank}: {disp_title} by {disp_author}")
