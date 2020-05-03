import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function that processes and inserts rows into the songs table and artists table. 
    
    INPUTS:
        cur: cursor object from the psycopg2 package
        filepath: str. path to the song's JSON file
    """
    
    
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 
                      'artist_name', 
                      'artist_location', 
                      'artist_latitude', 
                      'artist_longitude']].values[0].tolist()
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function that processes and inserts row data into the time, user and songplays tables. Time is converted from timestamp to datetimes. Songplays is contructed from JSON file contents and query data (found in sql_queries.py) for song_id and artist_id.
    
    INPUTS:
        cur: cursor object from the psycopg2 package
        filepath: str. path to the log JSON file
    """
    
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = df.ts.apply(lambda x: pd.Timestamp(x, unit='ms'))
    
    # insert time data records
    time_data = time_data = [t.values,
             t.apply(lambda x:x.hour).values.tolist(), 
             t.apply(lambda x:x.day).values.tolist(), 
             t.apply(lambda x:x.week).values.tolist(), 
             t.apply(lambda x:x.month).values.tolist(), 
             t.apply(lambda x:x.year).values.tolist(), 
             t.apply(lambda x:x.dayofweek).values.tolist()]
    
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    time_df = pd.DataFrame({column_labels[i]:time_data[i] for i in range(len(time_data))})

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
        songplay_data = (pd.Timestamp(row.ts, unit='ms'), 
                         row.userId, 
                         row.level, 
                         songid, 
                         artistid, 
                         row.sessionId, 
                         row.location, 
                         row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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