# NYT Bestsellers Lists Pipeline & Dashboard

This repo is my personal interest project data pipeline and dashboard for the NYT Besteller Lists both current and historical. This repo both 1) visualizes the current and historical NYT Bestseller Lists data -- with book rankings and info -- in a Streamlit app, and 2) collects data on the Bestseller Lists and uploads it to an internal database.

## ETL Data Pipeline

The sub-folder `src/etl/` as well as the folder `dags/` contain code that __collects NYT Bestseller List data from their public API, transforms it, and loads it to a cloud database__ (Azure SQL). The pipeline runs on a weekly cadence to cohere with the NYT's update schedule for their Bestseller Lists, and is executed through a scheduled GitHub Actions workflow that uses an ephemeral Airflow instance.

The repo also includes a separate workflow that executes a _backfill of historical data_, executing a specialized implementation of the pipeline over a list of dates to use in iterative API calls (see sub-folder `src/backfill/`). It runs only via manual dispatch and is programmed to dynamically infer the dates needed for the backfill by scanning the database for the earliest date and pairing this date with the known API date floor (June 4, 2008) to construct the date range. The workflow can also accept user-provided dates.

## Web App

The Streamlit app shows both __the current Bestseller Lists__ and __lists of the past that can be viewed by date filters__. The app view is separated into weekly and monthly lists to match the NYT's list separation format, with the date filter functionality being available for both list types.

The app can be found [here](https://nyt-bslists-viewer.streamlit.app/).

More features are planned to be added to the app soon. Please reach out to me at canyenheimuli@gmail.com or submit an issue if you have questions or requests concerning the app.
