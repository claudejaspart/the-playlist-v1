# generates the script needed for the database with following info:
# - shop name
# - users : admin and buyer
# - albums and artists of selected style
# - list of songs in each album

import discogs_client
from discogs_client.exceptions import HTTPError
import psycopg2
import random
from urllib.parse import quote, unquote
import os


# cleanup
os.system("rm -rf ./covers")
os.system("rm -rf ./db_scripts")

# utility functions
def downloadCover(image_uri, album) :
    content, resp= discogsclient._fetcher.fetch(None, "GET", image_uri, headers={"User-agent": discogsclient.user_agent})
    file = open(album, "wb")
    file.write(content)
    file.close()


# search variables
shop_name = os.getenv('SHOP_NAME')
total_albums = int(os.getenv('TOTAL_ALBUMS'))
music_style= os.getenv('MUSIC_STYLE')
print("The Playlist Database Generator")
print("-------------------------------")
print("")
print("Web app details : ")
print("Playlist app name : {}".format(shop_name))
print("Total albums : {}".format(str(total_albums)))
print("Music style : {}".format(music_style))
print("")

# API keys
consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token= os.getenv('ACCESS_TOKEN')
access_secret=os.getenv('ACCESS_SECRET')

# DB keys
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB').lower()
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

# Creating the folders
COVERS_DIR="./covers"
DB_SCRIPTS_DIR="./db_scripts"
os.makedirs(COVERS_DIR, exist_ok=True)
os.makedirs(DB_SCRIPTS_DIR, exist_ok=True)

# connecting to the database
print("Database setup")
print("--------------")
print("")
print("Connecting to database ...")
print("")
try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    print("Connection successful !")

    # Create a cursor object
    cursor = conn.cursor()

    # opening file
    file = open("{}/{}".format(DB_SCRIPTS_DIR, "create_database.sql"), "a")

    # Creating database 
    print("Creating database")
    query="CREATE DATABASE {}".format(shop_name)
    file.write("-- DATABASE CREATE\n")
    file.write(query)
    file.write("\n\n")

    # Creating SHOP 
    print("Generating the tables ...")
    print("")

    print("Creating table SHOP")
    query="CREATE TABLE shop(id  serial PRIMARY KEY,name VARCHAR(255),style VARCHAR(255));"
    file.write("-- TABLE CREATE\n")
    file.write(query)
    file.write("\n")
    cursor.execute(query)

    # Creating ALBUMS 
    print("Creating table ALBUMS")
    query="CREATE TABLE albums(id SERIAL PRIMARY KEY,title VARCHAR(2048),year VARCHAR(4),artist VARCHAR(255),labels VARCHAR(255),art VARCHAR(255));"
    file.write(query)
    file.write("\n")
    cursor.execute(query)

    # Commiting changes
    conn.commit()

    # adding shop name
    print("Adding shop data")
    query="insert into shop (name,style) "
    query=query+"values ('{}','{}');".format(quote(shop_name), quote(music_style))

    file.write("-- SHOP INFO INSERT\n")
    file.write(query)
    file.write("\n\n")
    # Exec query
    cursor.execute(query)
    conn.commit()


    # instantiate our discogs_client object
    print("")
    print("Connecting to the discogs API ...")
    discogsclient = discogs_client.Client("discogs_api_example/2.0", consumer_key=consumer_key, consumer_secret=consumer_secret, token=access_token, secret=access_secret)
    print("Connection successful")

    # search
    print("Searching the discogs database ...")
    search_results = discogsclient.search(type="release", style=music_style)
    print(search_results.pages)
    print(search_results.count)
    print("Search complete")

    # save results
    print("")
    print("Inserting albums in the DB")
    print("")
    file.write("-- ALBUMS insert\n")

    albums=1
    for release in search_results:
        # album data
        album_id=release.id
        album_artist="".join(quote(artist.name) for artist in release.artists)
        album_title=quote(release.title)
        album_year=release.year
        album_labels=quote("".join(label.name for label in release.labels))
        cover_filename="{}/{}.jpg".format(COVERS_DIR, quote(album_title[:15]))
        album_cover=downloadCover(release.images[0]["uri"], cover_filename)

        # inserting albums in the db
        query="insert into albums (title,artist,year,labels,art) "
        query=query+"values ('{}','{}',{},'{}','{}');".format(album_title, album_artist, album_year, album_labels, cover_filename)
        # Exec query
        print("inserting album {}/{}".format(albums, total_albums))
        file.write(query)
        file.write("\n")
        cursor.execute(query)
        conn.commit()

        # checking for number of albums
        if albums >= total_albums:
            file.close()
            break
        albums=albums+1

except (psycopg2.Error) as error:
    print("DB error : " , error)
except (Exception) as error:
    print("Error : " , error)