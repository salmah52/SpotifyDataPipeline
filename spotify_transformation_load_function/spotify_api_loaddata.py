import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import pandas as pd
from io import StringIO
from google.cloud import storage
import functions_framework

# Set your Spotify API client credentials as environment variables
os.environ['SPOTIPY_CLIENT_ID'] = 'f16ee809b17b409c8402ece04f3af48b'
os.environ['SPOTIPY_CLIENT_SECRET'] = '77791a6b2f924c1db10da6f4008e51e8'

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                            'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)
    return album_list


def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list


def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
        
    return song_list

@functions_framework.http
def spotify_api_loaddata(request):
    storage_client = storage.Client()
    bucket_name = "mainspotify02"
    prefix = "raw_data/to_processed/"
    
    bucket = storage_client.get_bucket(bucket_name)
    blob_list = list(bucket.list_blobs(prefix=prefix))
    
    spotify_data = []
    spotify_keys = []
    for blob in blob_list:
        if blob.name.split('.')[-1] == "json":
            content = blob.download_as_text()
            jsonObject = json.loads(content)
            spotify_data.append(jsonObject)
            spotify_keys.append(blob.name)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        #Song Dataframe
        song_df = pd.DataFrame.from_dict(song_list)
        
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        songs_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        
        bucket.blob(songs_key).upload_from_string(song_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        
        bucket.blob(album_key).upload_from_string(album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        
        bucket.blob(artist_key).upload_from_string(artist_content)
        
    for key in spotify_keys:
        source_blob = bucket.blob(key)
        destination_blob = bucket.blob('raw_data/processed/' + key.split("/")[-1])
        destination_blob.rewrite(source_blob)
        source_blob.delete()
    return f"Data uploaded to GCP Cloud Storage successfully! Processed {len(spotify_data)} files."

# For local testing

# For local testing
if __name__ == "__main__":
    functions_framework.start()
    #spotify_api_loaddata(None)
