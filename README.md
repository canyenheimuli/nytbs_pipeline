# NYT Bestsellers Lists Pipeline & Dashboard

This repo is my personal interest project data pipeline and dashboard for the NYT Besteller Lists both current and historical. This repo both 1) visualizes the current and historical NYT Bestseller Lists data -- with book rankings and info -- in a Streamlit app, and 2) collects data on updates to the Bestseller Lists and uploads it to an internal database on a regular schedule.

## ETL Pipeline

The sub-folder `/src/etl` as well as the folder `/dags` contain code that __collects NYT Bestseller List data from their public API, transforms it, and loads it to a cloud database__ (Azure SQL). The pipeline runs on a weekly cadence to cohere with the NYT's update schedule for their Bestseller Lists, and is executed through a scheduled GitHub Actions workflow that uses an ephemeral Airflow instance.

## Web App

The Streamlit app allows viewers to both __follow the current Bestseller Lists__ and __look at lists in the past using date filters__. The app view is separated into weekly and monthly lists to match the NYT's list separation format, and the date filter functionality is available for both list types.

The app can be found [here](https://nyt-bslists-viewer.streamlit.app/).

More features are planned to be added to the app soon. Please reach out to me or submit an issue if you have questions or requests concerning the app.
