from google.cloud import bigquery
from google.cloud import storage

# Set up BigQuery client and dataset
client = bigquery.Client.from_service_account_json(r'/home/lasisisalmah52/Load/service_account_key.json')
dataset_id = "Spotify009"
table_id = "Songs_data"

# Define schema for the table
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

# Create the table if it doesn't exist
table_ref = client.dataset(dataset_id).table(table_id)
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table, exists_ok=True)

# Load data into the table
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1  # Skip the header row
job_config.autodetect = True  # Automatically detect schema from the data
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND  # Append data to the table

bucket_name = "mainspotify02"
prefix = "transformed_data/songs_data/songs_transformed"

# Initialize Cloud Storage client
storage_client = storage.Client()

# Get the bucket and list the blobs with the given prefix
bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs(prefix=prefix)

# Load each blob (CSV file) into the table
for blob in blobs:
    uri = f"gs://{bucket_name}/{blob.name}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

print(f"Data loaded into BigQuery table: {table_id}")
