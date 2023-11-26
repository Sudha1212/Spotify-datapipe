import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import requests
from requests import post,get
import base64
import json

client_id='XXXX'
client_secret='XXXX'

def get_token():
    auth_string= client_id +":"+ client_secret
    auth_bytes= auth_string.encode("utf-8")
    auth_base64=str(base64.b64encode(auth_bytes),"utf-8")

    url='https://accounts.spotify.com/api/token'

    header= {
    "Authorization":"Basic " + auth_base64,
    "Content-Type": "application/x-www-form-urlencoded"
    }
    data ={"grant_type" :"client_credentials"}
    result= post(url,headers=header,data=data)
    json_result=json.loads(result.content)
    token = json_result["access_token"]

    return token


def get_auth_header(token):
    return { "Authorization" : "Bearer "+ token}


def search_for_artist(token,artist_name):
    url="https://api.spotify.com/v1/search"

    header1=get_auth_header(token)

    query=f"?q={artist_name}&type=artist&limit=1"

    query_dtls= url+query

    rslt=get(query_dtls,headers=header1)

    json_album=json.loads(rslt.content)['artists']['items']

    if len(json_album)==0:
        print(f"No artist with %d name",artist_name)
        return None

    return json_album[0]

    #print(json_album)


def get_songs_by_artist(token,artist_id):
  
   url=f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=IN"

   headers= get_auth_header(token)

   rslt= get(url=url,headers=headers)

   json_artist_songs= json.loads(rslt.content)["tracks"]

   return json_artist_songs


def run_spotify_etl():
    token=get_token()
    artist=search_for_artist(token,"Arjit")
    artist
    artist_id=artist['id']
    artist_tracks= get_songs_by_artist(token,artist_id)

    album_list =[]
    for idx,i in enumerate(artist_tracks):
        album_element={'album_id':artist_tracks[0]["album"]["id"],'album_name':artist_tracks[idx]["album"]["name"],'album_release_date':artist_tracks[idx]["album"]["release_date"],
                        'track_name':artist_tracks[idx]["name"],'track_uri':artist_tracks[idx]["external_urls"]}
        album_list.append(album_element)

    album_df=pd.DataFrame.from_dict(album_list)

    #album_df.info()

    #album_df.head()

    album_df.to_csv("s3://aws-mybuckets3/album_songs.csv")
