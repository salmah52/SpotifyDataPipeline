gcloud functions deploy Spotify_api_data_extract \
  --runtime=python39 \
  --region=us-east1 \
  --source=. \
  --entry-point=spotify_api_data \
  --trigger-http \
  --allow-unauthenticated