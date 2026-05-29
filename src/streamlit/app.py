# Packages
import streamlit as st
import viz_db as vdb

# Back-end (data queries)
weekly_lists = vdb.query_latest_weeklies(list_id=704) # TO-DO: Update with dynamic list ID retrieval
monthly_lists = vdb.query_latest_monthlies(list_id=532) # TO-DO: Update with dynamic list ID retrieval

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
    st.write("Weekly lists include \"Combined Fiction\", and monthly lists include \"Business and Self-help\".")

# Weeklies Tab
with tab_2:
    # Tab Title
    st.title("Weekly Lists")
    
    # Combined List Section
    st.header("Combined Print/E-Book Fiction")
    st_cols = st.columns(len(weekly_lists))

    # Loop parallel over inputs into st_cols
    for r_rank, r_title, r_author, r_image, st_col in zip(weekly_lists['rank'], weekly_lists['title'], weekly_lists['author'], weekly_lists['image'], st_cols):
        with st_col:
            st.image(r_image, width=100)
            st.markdown(f"{r_title} by {r_author} ({r_rank})")

# Monthlies Tab
with tab_3:
    # Tab Title
    st.title("Monthly Lists")
    
    # Business Section
    st.header("Business")
    st_cols = st.columns(len(monthly_lists))

    # Loop parallel over inputs into st_cols
    for r_rank, r_title, r_author, r_image, st_col in zip(monthly_lists['rank'], monthly_lists['title'], monthly_lists['author'], monthly_lists['image'], st_cols):
        with st_col:
            st.image(r_image, width=100)
            st.markdown(f"{r_title} by {r_author} ({r_rank})")
