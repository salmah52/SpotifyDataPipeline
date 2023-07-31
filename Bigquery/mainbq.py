import os
from google.cloud import bigquery
from google.cloud import storage

# Set your Google Cloud project ID
project_id = "celestial-gist-375110"

# Initialize BigQuery client
bq_client = bigquery.Client(project=project_id)

# Define the dataset ID where you want to store the data
dataset_id = "Spotify009"

def load_data_to_bigquery(data, table_id, schema):
    # Create BigQuery dataset if it does not exist
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        bq_client.create_dataset(dataset_ref)

    # Get the existing table, if it exists
    table_ref = dataset_ref.table(table_id)
    try:
        table = bq_client.get_table(table_ref)
    except Exception:
        table = None

    # Check if the table already exists
    if table is not None:
        # Check if the provided schema matches the existing schema
        if schema == table.schema:
            print(f"The schema for table {table_id} matches the provided schema.")
        else:
            print("The provided schema does not match the existing schema. Unable to add fields.")
    else:
        # Create the table with the provided schema if it does not exist
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)

        # Load data into the BigQuery table
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # If CSV has a header, otherwise set to 0
            autodetect=True,  # Detect schema automatically from data
        )

        job = bq_client.load_table_from_uri(
            data,
            table_ref,
            job_config=job_config
        )

        job.result()  # Wait for the job to complete

def spotify_api_loaddata(event, context):
    bucket_name = "mainspotify02"
    prefix = "transformed_data/"

    # Initialize Cloud Storage client
    storage_client = storage.Client()

    # Get the bucket and list the blobs with the given prefix
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.name.endswith(".csv"):
            if "songs_data" in blob.name:
                schema = [
                    bigquery.SchemaField("song_id", "STRING"),
                    bigquery.SchemaField("song_name", "STRING"),
                    bigquery.SchemaField("duration_ms", "INTEGER"),
                    bigquery.SchemaField("url", "STRING"),
                    bigquery.SchemaField("popularity", "INTEGER"),
                    bigquery.SchemaField("song_added", "TIMESTAMP"),
                    bigquery.SchemaField("album_id", "STRING"),
                    bigquery.SchemaField("artist_id", "STRING"),
                ]
                load_data_to_bigquery(f"gs://{bucket_name}/{blob.name}", "songs_data", schema)
            elif "artist_data" in blob.name:
                schema = [
                    # Define the schema for the artist_data table
                    bigquery.SchemaField("artist_id", "STRING"),
                    bigquery.SchemaField("artist_name", "STRING"),
                    bigquery.SchemaField("external_url", "STRING"),
                ]
                load_data_to_bigquery(f"gs://{bucket_name}/{blob.name}", "artist_data", schema)
            elif "album_data" in blob.name:
                schema = [
                    bigquery.SchemaField("album_id", "STRING"),
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("release_date", "DATE"),
                    bigquery.SchemaField("total_tracks", "INTEGER"),
                    bigquery.SchemaField("url", "STRING"),
                ]
                load_data_to_bigquery(f"gs://{bucket_name}/{blob.name}", "album_data", schema)

# For local testing
if __name__ == "__main__":
    spotify_api_loaddata(None, None)
