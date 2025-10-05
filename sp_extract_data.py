# Imports
from dotenv import load_dotenv 
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import sqlite3
import utilities

# Declare connection to spotify database
_db = sqlite3.connect('/usr/files/spotify_data/sp_data.db')
_c = _db.cursor()

# Initialise Environment variables for connection to spotify
load_dotenv('/usr/files/scripts/python/.credentials/.env') 

# Set scope of spotify access
scope = "playlist-read-private playlist-read-collaborative user-library-read user-top-read" 

# Connect to API attempting to use cache file. As CMD, follow instructions in putty
# Authentcates API access using environment variables and scope (environment variables must follow spotipy naming)
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,open_browser=False,cache_path='/usr/files/scripts/python/.credentials/.cache')) 

# Variables to store user details for later use
user = sp.current_user()
user_id = user['id'] 
user_name = user['display_name']

# Drop existing tables to recreate for multiple data sources (cannot be included in specific function)
_c.execute("""
    DROP TABLE IF EXISTS DIM_COLLECTIONS
    """)

_c.execute("""
    DROP TABLE IF EXISTS FACT_TRACKS
    """)

# Functions for each spotify API call
# Oriented around the API rather than the data / tables to reduce the amount of calls needed (i.e. focus on liked tracks means that liked collections and tracks populated using one call)
def get_liked_tracks():
    # Function to get currently liked tracks
    # Seperate variable to store track count for use in DIM_COLLECTIONS later on, takes record count value from FACT
    track_count = 0

    try:
        # Start by getting list of all tracks and initiating log
        utilities.get_start_info("FACT_TRACKS (Liked Tracks)")
        record_count = 0
        tracks = utilities.get_items(sp,sp.current_user_saved_tracks())

        _c.execute("""
        CREATE TABLE IF NOT EXISTS FACT_TRACKS(
            TRACK_ID TEXT,
            COLLECTION_ID TEXT,
            COLLECTION_POSITION INTEGER,
            COLLECTION_ADDED_DATE TEXT)
        """)


        # For every track
        for track in tracks:
            # Get the track info needed for the FACT table (id, source and source details)
            record_count = record_count + 1
            TRACK = {
                "TRACK_ID":(track['track']['id']),
                "COLLECTION_ID":"LIKED_TRACKS",
                "COLLECTION_POSITION" : record_count,
                "COLLECTION_ADDED_DATE" : track['added_at']
            }

            # Insert into fact table
            _c.execute("""
                    INSERT INTO FACT_TRACKS (
                        TRACK_ID,
                        COLLECTION_ID,
                        COLLECTION_POSITION,
                        COLLECTION_ADDED_DATE)
                    VALUES(
                        :TRACK_ID, 
                        :COLLECTION_ID,
                        :COLLECTION_POSITION,
                        :COLLECTION_ADDED_DATE)  
                """,TRACK)
        
        # Commit changes once all tracks are inserted
        _db.commit()

        # Log finish
        utilities.get_finish_info(record_count)
        track_count = record_count
        utilities.write_log()


    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()
        _db.commit()
    
    try:
        # Function to store info of collection 
        utilities.get_start_info("DIM_COLLECTION (Liked Tracks)")
        # Only one collection needed, still logged
        record_count = 1

        _c.execute("""
        CREATE TABLE IF NOT EXISTS DIM_COLLECTIONS(
            ID TEXT,
            NAME TEXT,
            OWNER TEXT,
            TYPE TEXT,
            TRACK_COUNT INTEGER,
            PUBLIC INTEGER,
            OWNED INTEGER)
        """)


        # Create dict of Liked tracks collection, everything hardcoded excep the track count and username
        LIKED_TRACKS = {
            "ID": "LIKED_TRACKS",
            "NAME": "Liked Tracks",
            "TYPE": "preset",
            "OWNER": user_name,
            "TRACK_COUNT": track_count,
            "PUBLIC": 0,
            "OWNED": 1
        }

        # Insert into dim collections
        _c.execute("""
                INSERT INTO DIM_COLLECTIONS (
                    ID,
                    NAME,
                    TYPE, 
                    OWNER, 
                    TRACK_COUNT,
                    PUBLIC, 
                    OWNED)
                VALUES(
                    :ID, 
                    :NAME, 
                    :TYPE, 
                    :OWNER,
                    :TRACK_COUNT, 
                    :PUBLIC, 
                    :OWNED)  
            """,LIKED_TRACKS)   
        _db.commit()

        utilities.get_finish_info(record_count)
        utilities.write_log()

    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

def get_top_tracks(time_range):
    # Function to get top tracks, follows liked tracks pattern only uses parameter for time range parameter in API
    # Minor alterations need seperate function despite close pattenr
    track_count = 0
    
    try:
        utilities.get_start_info(f"FACT_TRACKS (Top Tracks - {time_range})")
        record_count = 0
        # Limit set to 50 as defaults to 20, but 50 max. Reduces calls needed
        tracks = utilities.get_items(sp,sp.current_user_top_tracks(limit=50, time_range=time_range))

        _c.execute("""
        CREATE TABLE IF NOT EXISTS FACT_TRACKS(
            TRACK_ID TEXT,
            COLLECTION_ID TEXT,
            COLLECTION_POSITION INTEGER,
            COLLECTION_ADDED_DATE TEXT)
        """)

        # Rate limit was often exceeded due to amount of top tracks included, limited to avoid this
        # Also after a certain amount, data became meaningless. Mightve included every track ever listened to in long_term
        track_limit = 0
        match time_range:
            case "short_term":
                track_limit = 250
            case "medium_term":
                track_limit = 500
            case "long_term":
                track_limit = 1000

        tracks = tracks[:track_limit]

        for track in tracks:
            record_count = record_count + 1

            TRACK = {
                "TRACK_ID":(track['id']),
                "COLLECTION_ID":f"TOP_TRACKS_{time_range}",
                "COLLECTION_POSITION":(record_count)
            }

            _c.execute("""
                    INSERT INTO FACT_TRACKS (
                        TRACK_ID,
                        COLLECTION_ID,
                        COLLECTION_POSITION)
                    VALUES(
                        :TRACK_ID, 
                        :COLLECTION_ID,
                        :COLLECTION_POSITION)  
                """,TRACK)
        
        _db.commit()
        
        utilities.get_finish_info(record_count)
        track_count = record_count
        utilities.write_log()

    
    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

    try:
        utilities.get_start_info(f"DIM_COLLECTION (Top Tracks - {time_range})")
        record_count = 1

        _c.execute("""
        CREATE TABLE IF NOT EXISTS DIM_COLLECTIONS(
            ID TEXT,
            NAME TEXT,
            OWNER TEXT,
            TYPE TEXT,
            TRACK_COUNT INTEGER,
            PUBLIC INTEGER,
            OWNED INTEGER)
        """)

        TOP_TRACKS = {
            "ID": f"TOP_TRACKS_{time_range}",
            "NAME": f"Top Tracks - {time_range}",
            "TYPE": "preset",
            "OWNER": user_name,
            "TRACK_COUNT": track_count,
            "PUBLIC": 0,
            "OWNED": 1
        }

        _c.execute("""
                INSERT INTO DIM_COLLECTIONS (
                    ID,
                    NAME,
                    TYPE, 
                    OWNER, 
                    TRACK_COUNT,
                    PUBLIC, 
                    OWNED)
                VALUES(
                    :ID, 
                    :NAME, 
                    :TYPE, 
                    :OWNER,
                    :TRACK_COUNT, 
                    :PUBLIC, 
                    :OWNED)  
            """,TOP_TRACKS)
        
        _db.commit()

        utilities.get_finish_info(record_count)
        utilities.write_log()

    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

def get_playlists():
    # Function to get playlist. Reverse pattern to Liked and Top, gets collections first as API doesnt give tracks immediately

    # List to store playlist IDs for later use
    playlist_ids = []
    try:
        # Function to get all playlist info, standard collection process
        utilities.get_start_info("DIM_COLLECTIONS (Playlists)")
        record_count = 0

        _c.execute("""
        CREATE TABLE IF NOT EXISTS DIM_COLLECTIONS(
            ID TEXT,
            NAME TEXT,
            OWNER TEXT,
            TYPE TEXT,
            TRACK_COUNT INTEGER,
            PUBLIC INTEGER,
            OWNED INTEGER)
        """)

        playlists = utilities.get_items(sp,sp.current_user_playlists())

        for playlist in playlists:

            playlist_ids.append(playlist['id'])

            PLAYLIST = {
                "ID": playlist['id'],
                "NAME": playlist['name'],
                "TYPE": playlist['type'],
                "OWNER": playlist['owner']['display_name'],
                "TRACK_COUNT": playlist['tracks']['total'],
                "PUBLIC": 1 if playlist['public'] == True else 0,
                "OWNED": 1 if playlist['owner']['display_name'] == user_name else 0
            }

            record_count = record_count + 1

            _c.execute("""
                    INSERT INTO DIM_COLLECTIONS (
                        ID,
                        NAME,
                        TYPE, 
                        OWNER, 
                        TRACK_COUNT,
                        PUBLIC, 
                        OWNED)
                    VALUES(
                        :ID, 
                        :NAME, 
                        :TYPE, 
                        :OWNER,
                        :TRACK_COUNT, 
                        :PUBLIC, 
                        :OWNED)  
                """,PLAYLIST)
        
        _db.commit()

        utilities.get_finish_info(record_count)
        utilities.write_log()

    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

    try:
        utilities.get_start_info("FACT_TRACKS (Playlists)")

        # Iterates through each playlist ID, gets each playlist tracks then iterates through that
        for id in playlist_ids:
            record_count = 0
            tracks = (utilities.get_items(sp,sp.playlist(id)['tracks']))

            for track in tracks:
                record_count = record_count + 1

                # For some playlists, some tracks are considered NONE which throws exception unless handled
                if track['track'] is not None:
                    TRACK = {
                        "TRACK_ID": (track['track']['id']),
                        "COLLECTION_ID":id,
                        "COLLECTION_POSITION":(record_count),
                        "COLLECTION_ADDED_DATE": track['added_at']
                    }

                    _c.execute("""
                        INSERT INTO FACT_TRACKS (
                            TRACK_ID,
                            COLLECTION_ID,
                            COLLECTION_POSITION,
                            COLLECTION_ADDED_DATE)
                        VALUES(
                            :TRACK_ID, 
                            :COLLECTION_ID,
                            :COLLECTION_POSITION,
                            :COLLECTION_ADDED_DATE)  
                    """,TRACK)
        
        _db.commit()
        utilities.get_finish_info(record_count)
        utilities.write_log()

    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

def get_track_info():
    # Function to iterate through all distinct tracks populated in the fact tables

    try:
        _c.execute("""
        DROP TABLE IF EXISTS DIM_TRACKS
        """)

        _c.execute("""
            CREATE TABLE IF NOT EXISTS DIM_TRACKS(
                TRACK_ID TEXT,
                TRACK_NAME TEXT,
                PRIMARY_ARTIST_ID TEXT,
                ALBUM_ID TEXT,
                DURATION_MS INTEGER,
                POPULARITY INTEGER,
                TYPE TEXT,
                MOST_RECENT_ADDED_DATE TEXT,
                PLAYLIST_COUNT INTEGER,
                TRACK_RANK REAL,
                IS_LIKED INTEGER)
            """)
        # Start by getting list of all unuique tracks and initiating log
        utilities.get_start_info("DIM_TRACKS")
        record_count = 0

        # Query to return each unique track ID
        # Also return the most recently added date to a collection (if available)
        # Also return the count of private playlists that the track is added to. Private is used to distinguished shared and none shared playlists to avoid corruption of friends bad taste
        # Also return a metric based on track position in top list if existing in list. Combines positiioning and prioritises the longest term list by weighting. 
        _c.execute("""
            WITH 
            TRACK_LIST AS 
            (SELECT DISTINCT TRACK_ID, MIN(COLLECTION_ADDED_DATE) AS MOST_RECENT_ADDED_DATE
            FROM FACT_TRACKS GROUP BY TRACK_ID),
            PLAYLIST_COUNT AS
            (SELECT DISTINCT TRACK_ID, COUNT(COLLECTION_ID) AS PLAYLIST_COUNT, NAME
            FROM FACT_TRACKS AS ft
            INNER JOIN DIM_COLLECTIONS AS dc ON ft.COLLECTION_ID = dc.ID
            WHERE COLLECTION_ID NOT LIKE "TOP_TRACKS%" AND COLLECTION_ID != "LIKED_TRACKS" AND PUBLIC = 0 AND NAME != "Autism"
            GROUP BY TRACK_ID),
            TRACK_RANK AS
            (SELECT TRACK_ID, (SUM(TRACK_RANK) * COLLECTION_WEIGHT) AS TRACK_RANK
            FROM
            (
            SELECT 
                TRACK_ID, 
                1-((COLLECTION_POSITION*1.0) / (COUNT(*) OVER (PARTITION BY COLLECTION_ID))*1.0) AS TRACK_RANK,
                CASE COLLECTION_ID
                WHEN 'TOP_TRACKS_long_term' THEN 0.8
                WHEN 'TOP_TRACKS_medium_term' THEN 0.4
                WHEN 'TOP_TRACKS_short_term' THEN 0.2
                END AS COLLECTION_WEIGHT
            FROM FACT_TRACKS
            WHERE COLLECTION_ID LIKE "TOP_TRACKS%")
            GROUP BY TRACK_ID),
            IS_LIKED AS
            (SELECT TRACK_ID, 1 AS IS_LIKED
            FROM FACT_TRACKS
            WHERE COLLECTION_ID = 'LIKED_TRACKS')

            SELECT tl.TRACK_ID, tl.MOST_RECENT_ADDED_DATE, pc.PLAYLIST_COUNT, tr.TRACK_RANK, il.IS_LIKED
            FROM TRACK_LIST AS tl
            LEFT JOIN PLAYLIST_COUNT AS pc ON tl.TRACK_ID = pc.TRACK_ID
            LEFT JOIN TRACK_RANK AS tr ON tl.TRACK_ID = tr.TRACK_ID
            LEFT JOIN IS_LIKED AS il ON tl.TRACK_ID = il.TRACK_ID
        """)
        tracks = _c.fetchall()

        # API handles tracks 50 (max) at a time. Chunk SQL result into 50s
        tracks_chunks = (list(utilities.chunk_list(tracks,50)))


        for tracks_chunk in tracks_chunks:
            chunk_ids = []
            chunk_base_track_details = []
            chunk_api_track_details = []

            # Iterates through each chunks. Gets ID and also gets query metrics
            for track in tracks_chunk:
                track_id = track[0]
                # Build list of 50 ids for the API
                chunk_ids.append(track_id)
                most_recent_added_date = track[1]
                playlist_count = track[2]
                track_rank = track[3]
                is_liked = track[4]

                # Builds dict with info gathered from SQL query result
                BASE_TRACK_DETAILS = {
                    "TRACK_ID" : track_id,
                    "MOST_RECENT_ADDED_DATE" : most_recent_added_date,
                    "PLAYLIST_COUNT" : playlist_count,
                    "TRACK_RANK" : track_rank,
                    "IS_LIKED" : is_liked
                } 

                # Add the dict to a list of track details specific to SQL results
                chunk_base_track_details.append(BASE_TRACK_DETAILS)
            
            # Get API results using list of IDS, no need for get items utility as already chunked to limit
            tracks = (sp.tracks(chunk_ids))['tracks']
            
            for track in tracks:
                track_id = track['id']
                track_name = track['name']
                primary_artist_id = track['artists'][0]['id']
                album_id = track['album']['id']
                duration_ms = track['duration_ms']
                popularity = track['popularity']
                type = track['type']

                # Build dict for results from API
                API_TRACK_DETAILS = {
                    "TRACK_ID" : track_id,
                    "TRACK_NAME" : track_name,
                    "PRIMARY_ARTIST_ID" : primary_artist_id,
                    "ALBUM_ID" : album_id,
                    "DURATION_MS" : duration_ms,
                    "POPULARITY" : popularity,
                    "TYPE" : type
                }

                # Add the dict to a list of track details specific to API results
                chunk_api_track_details.append(API_TRACK_DETAILS)
            
            for i in range(len(chunk_ids)):
                # For loop iterating through lenght of the current chunk

                # Build a combined track dict
                TRACK_DETAILS = {
                    "TRACK_ID" : chunk_api_track_details[i]['TRACK_ID'],
                    "TRACK_NAME" : chunk_api_track_details[i]['TRACK_NAME'],
                    "PRIMARY_ARTIST_ID" : chunk_api_track_details[i]['PRIMARY_ARTIST_ID'],
                    "ALBUM_ID" : chunk_api_track_details[i]['ALBUM_ID'],
                    "DURATION_MS" : chunk_api_track_details[i]['DURATION_MS'],
                    "POPULARITY" : chunk_api_track_details[i]['POPULARITY'],
                    "TYPE" : chunk_api_track_details[i]['TYPE'],
                    "MOST_RECENT_ADDED_DATE" : chunk_base_track_details[i]['MOST_RECENT_ADDED_DATE'],
                    "PLAYLIST_COUNT" : chunk_base_track_details[i]['PLAYLIST_COUNT'],
                    "TRACK_RANK" : chunk_base_track_details[i]['TRACK_RANK'],
                    "IS_LIKED" : chunk_base_track_details[i]['IS_LIKED']
                }

                record_count += 1

                # Insert into track details table
                _c.execute("""
                    INSERT INTO DIM_TRACKS (
                        TRACK_ID,
                        TRACK_NAME,
                        PRIMARY_ARTIST_ID, 
                        ALBUM_ID, 
                        DURATION_MS,
                        POPULARITY, 
                        TYPE,
                        MOST_RECENT_ADDED_DATE, 
                        PLAYLIST_COUNT,
                        TRACK_RANK, 
                        IS_LIKED)
                    VALUES(
                        :TRACK_ID,
                        :TRACK_NAME,
                        :PRIMARY_ARTIST_ID, 
                        :ALBUM_ID, 
                        :DURATION_MS,
                        :POPULARITY, 
                        :TYPE,
                        :MOST_RECENT_ADDED_DATE, 
                        :PLAYLIST_COUNT,
                        :TRACK_RANK, 
                        :IS_LIKED)  
                """,TRACK_DETAILS)

        _db.commit()

        utilities.get_finish_info(record_count)
        utilities.write_log() 

    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

def get_albums():
     # Function to iterate through all distinct albums populated in the fact tables. Chunked into max API allowance similar to tracks function
    try:
        _c.execute("""
        DROP TABLE IF EXISTS DIM_ALBUMS
        """)

        _c.execute("""
            CREATE TABLE IF NOT EXISTS DIM_ALBUMS(
                ALBUM_ID TEXT,
                ALBUM_NAME TEXT,
                ALBUM_TYPE TEXT,
                ALBUM_RELEASE TEXT,
                TOTAL_TRACKS INTEGER,
                TRACKS_ADDED INTEGER,
                ADDED_PERCENT REAL)
            """)
        
        # Start by getting list of all unique albums and the count of tracks from each album stored in playlist
        utilities.get_start_info("DIM_ALBUMS")
        record_count = 0

        # Query to return each unique album ID
        # Also return the count of tracks from each album stored
        _c.execute("""
            SELECT ALBUM_ID, COUNT(TRACK_ID) 
            FROM DIM_TRACKS
            WHERE PLAYLIST_COUNT IS NOT NULL
            GROUP BY ALBUM_ID
        """)

        albums = _c.fetchall()
        albums_chunks = (list(utilities.chunk_list(albums,20)))

        for albums_chunk in albums_chunks:
            chunk_ids = []
            chunk_base_album_details = []
            chunk_api_album_details = []

            for album in albums_chunk:

                # Build album details dict from sql results and api
                ALBUM_BASE_DETAILS = {
                    "ALBUM_ID": album[0],
                    "TRACKS_ADDED": album[1]
                }

                chunk_base_album_details.append(ALBUM_BASE_DETAILS)
                chunk_ids.append(album[0])

            albums = sp.albums(chunk_ids)['albums']

            for album in albums:
                ALBUM_API_DETAILS = {
                    "ALBUM_ID": album['id'],
                    "ALBUM_NAME": album['name'],
                    "ALBUM_TYPE": album['album_type'],
                    "ALBUM_RELEASE": album['release_date'],
                    "TOTAL_TRACKS": album['total_tracks']
                }

                chunk_api_album_details.append(ALBUM_API_DETAILS)


            for i in range(len(chunk_ids)):

                ALBUM_DETAILS = {
                    "ALBUM_ID": chunk_base_album_details[i]['ALBUM_ID'],
                    "ALBUM_NAME": chunk_api_album_details[i]['ALBUM_NAME'],
                    "ALBUM_TYPE": chunk_api_album_details[i]['ALBUM_TYPE'],
                    "ALBUM_RELEASE": chunk_api_album_details[i]['ALBUM_RELEASE'],
                    "TOTAL_TRACKS": chunk_api_album_details[i]['TOTAL_TRACKS'],
                    "TRACKS_ADDED": chunk_base_album_details[i]['TRACKS_ADDED'],
                    "ADDED_PERCENT":chunk_base_album_details[i]['TRACKS_ADDED']/chunk_api_album_details[i]['TOTAL_TRACKS']
                }
                
                
                record_count += 1

                # Insert into album details table
                _c.execute("""
                    INSERT INTO DIM_ALBUMS (
                        ALBUM_ID,
                        ALBUM_NAME,
                        ALBUM_TYPE,
                        ALBUM_RELEASE, 
                        TOTAL_TRACKS, 
                        TRACKS_ADDED,
                        ADDED_PERCENT)
                    VALUES(
                        :ALBUM_ID,
                        :ALBUM_NAME,
                        :ALBUM_TYPE,
                        :ALBUM_RELEASE, 
                        :TOTAL_TRACKS, 
                        :TRACKS_ADDED,
                        :ADDED_PERCENT)  
                """,ALBUM_DETAILS)

        _db.commit()

        utilities.get_finish_info(record_count)
        utilities.write_log() 

    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

def get_artists():
     # Function to iterate through all distinct tracks populated in the fact tables

    try:
        _c.execute("""
        DROP TABLE IF EXISTS DIM_ARTISTS
        """)

        _c.execute("""
            CREATE TABLE IF NOT EXISTS DIM_ARTISTS(
                ARTIST_ID TEXT,
                ARTIST_NAME TEXT,
                ARTIST_TYPE TEXT,
                ARTIST_POPULARITY INTEGER,
                TRACKS_ADDED INTEGER)
            """)
        
        # Start by getting list of all unique artists and the count of tracks from each artist stored in playlist
        utilities.get_start_info("DIM_ARTISTS")
        record_count = 0

        # Query to return each unique artist ID
        # Also return the count of tracks from each artist stored
        _c.execute("""
            SELECT PRIMARY_ARTIST_ID, COUNT(TRACK_ID) 
            FROM DIM_TRACKS
            WHERE PLAYLIST_COUNT IS NOT NULL
            GROUP BY PRIMARY_ARTIST_ID
        """)
        artists = _c.fetchall()

        arists_chunks = (list(utilities.chunk_list(artists,50)))

        for artists_chunk in arists_chunks:
            chunk_ids = []
            chunk_base_artist_details = []
            chunk_api_artist_details = []

            for artist in artists_chunk:

                # Build album details dict from sql results and api
                ARTIST_BASE_DETAILS = {
                    "ARTIST_ID": artist[0],
                    "TRACKS_ADDED": artist[1]
                }

                chunk_base_artist_details.append(ARTIST_BASE_DETAILS)
                chunk_ids.append(artist[0])

            artists = sp.artists(chunk_ids)['artists']

            for artist in artists:
                ARTIST_API_DETAILS = {
                    "ARTIST_ID": artist['id'],
                    "ARTIST_NAME": artist['name'],
                    "ARTIST_TYPE": artist['type'],
                    "ARTIST_POPULARITY": artist['popularity']
                }

                chunk_api_artist_details.append(ARTIST_API_DETAILS)
            
            for i in range(len(chunk_ids)):

                # Build artist details dict from sql results and api
                ARTIST_DETAIL = {
                    "ARTIST_ID": chunk_base_artist_details[i]['ARTIST_ID'],
                    "ARTIST_NAME": chunk_api_artist_details[i]['ARTIST_NAME'],
                    "ARTIST_TYPE": chunk_api_artist_details[i]['ARTIST_TYPE'],
                    "ARTIST_POPULARITY": chunk_api_artist_details[i]['ARTIST_POPULARITY'],
                    "TRACKS_ADDED": chunk_base_artist_details[i]['TRACKS_ADDED']
                }

                record_count += 1

                # Insert into artist details table
                _c.execute("""
                    INSERT INTO DIM_ARTISTS (
                        ARTIST_ID,
                        ARTIST_NAME,
                        ARTIST_TYPE,
                        ARTIST_POPULARITY, 
                        TRACKS_ADDED)
                    VALUES(
                        :ARTIST_ID,
                        :ARTIST_NAME,
                        :ARTIST_TYPE,
                        :ARTIST_POPULARITY, 
                        :TRACKS_ADDED)  
                """,ARTIST_DETAIL)

        _db.commit()

        utilities.get_finish_info(record_count)
        utilities.write_log() 

    except Exception as e:
        utilities.get_error_message(e.args[0])
        utilities.write_log()

        _db.commit()

get_top_tracks("short_term")
get_top_tracks("medium_term")
get_top_tracks("long_term")
get_liked_tracks()
get_playlists()
get_track_info()
get_albums()
get_artists()
_db.close()