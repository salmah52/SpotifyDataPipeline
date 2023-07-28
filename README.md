# SpotifyDataPipeline
# Spotify Data Engineering Project - Data Pipeline

## Introduction

The goal of this project is to create an end-to-end data pipeline that integrates with the Spotify API, extracts data from a specific playlist, processes it, and loads it into Google Cloud Storage and Google BigQuery. The pipeline is designed to be fully automated using Google Cloud Platform (GCP) services.

The project consists of several components, including data extraction, transformation, data movement, and loading into BigQuery. Each component plays a crucial role in ensuring the data is processed efficiently, organized, and readily available for analysis.


---
## Architecture

![image](https://github.com/salmah52/SpotifyDataPipeline/assets/44398948/fd2ae5ff-75cd-4bde-b8f3-5b9a6b836069)


---
## Technology Used
**Programming Language**
- Python

**Google Cloud Platform**
- Google Cloud Functions
- Google Cloud Storage
- Google BigQuery
- Cloud Scheduler

**Data Visualization**
- Power BI
---
## Project Structure
The project is organized into several components:

1. Extraction from Spotify API (Code in spotify_api_data.py): This script uses the Spotify API to extract data from a specific playlist and stores the raw data as JSON files in the "raw_data_unprocessed" bucket in Google Cloud Storage. The code is deployed as a Google Cloud Function and triggered using Cloud Scheduler on a daily basis.

2. Data Transformation (Code in spotify_api_loaddata.py): This script reads the raw data files from the "raw_data_unprocessed" bucket, performs data transformation, and creates three separate CSV files for albums, artists, and songs. The transformed data is then stored in the "transformed_data" bucket in Google Cloud Storage.

3. Data Movement and Cleanup (Code in spotify_api_loaddata.py): This Cloud Function is triggered whenever new data is uploaded to the "raw_data_unprocessed" bucket. It automatically moves the newly uploaded raw data files to their respective folders ("album_data", "artist_data", "songs_data") in the "transformed_data" bucket. After the data movement is complete, the original raw data files are deleted from the "raw_data_unprocessed" bucket to avoid duplicate processing.

4. Loading Data into BigQuery (Code in spotify_api_loaddata.py): Once the data is transformed and stored in the "transformed_data" bucket, it is further loaded into Google BigQuery for data analysis. The code creates three tables for albums, artists, and songs in the "Spotify009" dataset in BigQuery. The data is appended to these tables on each run to maintain a complete historical record.

---
## Automated Pipeline
The entire data pipeline is designed to be automated and scheduled to run daily. The Google Cloud Function responsible for data extraction is triggered by Cloud Scheduler on a daily basis. This ensures that the latest data from the Spotify API is fetched regularly.

Upon successful extraction, the transformation process is triggered automatically for the newly uploaded data. The transformed data is then moved to their respective folders, and the raw data is removed to keep the data clean and organized.

Finally, the data is loaded into Google BigQuery, providing a centralized and scalable database for performing data analytics and generating insights.
---
## Conclusion

By implementing this fully automated data pipeline, we ensure that new data from the Spotify API is efficiently processed and made available for analysis. This project showcases the power and versatility of Google Cloud Platform in building robust and automated data engineering solutions.

*Happy data engineering!*

