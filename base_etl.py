import os
from dotenv import load_dotenv
import spotify.sync as spotify
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime, timedelta
from psycopg2 import DatabaseError
from sqlalchemy import create_engine
import subprocess

load_dotenv(os.path.join(os.path.abspath(os.path.dirname("base_etl.py"))+"\etl", '.env'))
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")
user = os.environ.get("USER")
pwd = os.environ.get("PWD")
port = os.environ.get("PORT")
host = os.environ.get("HOST")
db = os.environ.get("DB")
nombre_contenedor_postgres = os.environ.get("NOMBRE_CONTENEDOR_POSTGRES")
nombre_contenedor_pgadmin = os.environ.get("NOMBRE_CONTENEDOR_PGADMIN")

def ejecutar_docker_compose(ruta_compose):
    # Comando para ejecutar Docker Compose con la ruta del archivo .yml
    comando = f"docker-compose -f {ruta_compose} up -d"
    # Ejecutar el comando en segundo plano
    subprocess.run(comando, shell=True)
    print("Contenedores creados exitosamente.")

def crear_engine(user,pwd,host,port,db):
    try:
        engine = create_engine(
            'postgresql+psycopg2://'+user+':'+pwd+'@'+host+':'+port+'/'+db+'')
        print("engine ok")
    except:
        print("I can't create engine")
    return engine

def get_artist(name):
    results = sp.search(q='artist:' + name, type='artist')
    items = results['artists']['items']
    if len(items) > 0:
        return items[0]
    else:
        return None

def show_album_tracks(album):
    tracks = []
    results = sp.album_tracks(album['id'])
    tracks.extend(results['items'])
    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])

def show_artist_albums(artist):
    albums = []
    results = sp.artist_albums(artist['id'], album_type='album')
    albums.extend(results['items'])
    tracks = []
    while results['next']:
        results = sp.next(results)
        albums.extend(results['items'])
    unique = set()  # skip duplicate albums
    for album in albums:
        name = album['name'].lower()
        if name not in unique:
            unique.add(name)
            results = sp.album_tracks(album['id'])
            tracks.extend(results['items'])
            while results['next']:
                results = sp.next(results)
                tracks.extend(results['items'])

            
    return unique,tracks

def extract(artist, limit=50):
    artista = get_artist(artist)
    a,b=show_artist_albums(artista)
    return a,b

def transform(data):
    df=pd.DataFrame(data)
    return df

def load(df,engine,table_name):
    df = df.astype(str)
    try:
        df.astype(str).to_sql(table_name, engine, if_exists='replace',index=False)
        print("load ok")
    except:
        print("fallo load")
    return

if __name__ == "__main__":
    #creacion cliente spotify
    # client = spotify.Client(client_id, client_secret)
    scope='user-read-recently-played'
    sp= spotipy.Spotify(auth_manager=SpotifyOAuth(client_id,client_secret,redirect_uri="http://localhost:5000",scope=scope))
    
    # Ruta del archivo .yml de Docker Compose
    ruta_docker_compose = "etl/docker-compose.yml"
    # Llamar a la función para ejecutar Docker Compose
    ejecutar_docker_compose(ruta_docker_compose)
    
    #engine postgres db
    engine=crear_engine(user,pwd,host,port,db)
    
    #ETL
    albums,tracks=extract('Drake')
    df=transform(tracks)
    load(df,engine,table_name='prueba2')