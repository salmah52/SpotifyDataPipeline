gcloud functions deploy Spotify_api_data_load \
  --runtime=python39 \
  --region=us-east1 \
  --source=. \
  --entry-point=spotify_api_loaddata \
  --allow-unauthenticated