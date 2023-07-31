import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
from google.cloud import storage
import functions_framework

# Set your Spotify API client credentials as environment variables
os.environ['SPOTIPY_CLIENT_ID'] = ''
os.environ['SPOTIPY_CLIENT_SECRET'] = ''


@functions_framework.http
def spotify_api_data(request):
    client_id = os.environ.get('SPOTIPY_CLIENT_ID')
    client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"

    # Spotify setup
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    spotify_data = sp.playlist_tracks(playlist_URI)

    # GCP Cloud Storage setup
    bucket_name = "mainspotify02"
    filename = "spotify_raw_" + str(datetime.now()) + ".json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("raw_data/to_processed/" + filename)
    blob.upload_from_string(json.dumps(spotify_data))

    return "Data uploaded to GCP Cloud Storage successfully!"

if __name__ == "__main__":
    functions_framework.start()
