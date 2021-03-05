import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
    This procedure processes a song file whose filepath has been provided as an argument.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
"""

def process_song_file(cur, filepath):
    """
    Process songs files and insert records into the Postgres database.
    INPUTS: 
    * cur : cursor reference
    * filepath : complete file path for the file to load
    """
        
    # open song file
    df = pd.read_json( filepath, lines=True)

    for _,row in df.iterrows() : 
        # insert song record
        song_data = ( row.song_id, row.title, row.artist_id, row.year, row.duration)
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = ( row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude)
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process Event log files and insert records into the Postgres database.
    INPUTS:
    * cur: cursor reference
    * filepath: complete file path for the file to load
    """
    
    # open log file
    df = pd.read_json( filepath, lines=True)

    # filter by NextSong action
    df = df.loc[(df.page == 'NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime( df.ts, unit='ms')
    
    # insert time data records
    time_data = ( t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ( 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict( dict( zip( column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime( row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Driver function to load data from songs and event log files into Postgres database.
    INPUTS:
    * cur: a database cursor reference
    * conn: database connection reference
    * filepath: parent directory where the files exists 
    * func: function to call
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()