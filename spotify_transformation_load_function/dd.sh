gcloud functions deploy spotify_api_loaddata \
    --runtime python310 \
    --trigger-resource mainspotify02 \
    --trigger-event google.storage.object.finalize \
    --entry-point spotify_api_loaddata