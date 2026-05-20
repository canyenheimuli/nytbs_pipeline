import streamlit as st

st.set_page_config(page_title = "NYT BS Lists Dashboard", layout = "wide")
st.title("Combined Fiction")

imgs = [
    "https://static01.nyt.com/bestsellers/images/9781668236512.jpg", # Theo of Golden
    "https://static01.nyt.com/bestsellers/images/9780593135228.jpg", # PHM
    "https://static01.nyt.com/bestsellers/images/9781728289779.jpg", # King of Gluttony???
    "https://static01.nyt.com/bestsellers/images/9780593804216.jpg", # Yesteryear
    "https://static01.nyt.com/bestsellers/images/9780593798430.jpg" # The Correspondent
]

titles = [
    "Theo of Golden",
    "Project Hail Mary",
    "King of Gluttony",
    "Yesteryear",
    "The Correspondent"
]

st.markdown('<meta http-equiv="refresh" content="30">', unsafe_allow_html=True)

cols = st.columns(len(imgs))

for img, title, rank, col in zip(imgs, titles, range(1, 6), cols):
    with col:
        st.image(img, width = 100)
        st.markdown(f"{title} ({rank})")
