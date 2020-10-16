import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Insert the Song and Artist data reside on the file path into the tables of the database the cursor pointing at.

            Parameters:
                    cur (cursor): A connection cursor
                    filepath (String): A string containing the path to the file

            Returns:
                    None
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Insert the Time, User and Songplay data reside on the file path into the tables of the database the cursor pointing at.

            Parameters:
                    cur (cursor): A connection cursor
                    filepath (String): A string containing the path to the file

            Returns:
                    None
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"] 

    # convert timestamp column to datetime
    df.ts = pd.to_datetime(df.ts, unit='ms')
    df["t"] = df.ts.astype('int64') // 10**9
    df["h"] = pd.Series(df["ts"]).dt.hour
    df["d"] = pd.Series(df["ts"]).dt.day
    df["wy"] = pd.Series(df["ts"]).dt.weekofyear
    df["m"] = pd.Series(df["ts"]).dt.month
    df["y"] = pd.Series(df["ts"]).dt.year
    df["wd"] = pd.Series(df["ts"]).dt.weekday
    
    # insert time data records
    time_data = df[['t','h','d','wy','m','y','wd']].values.tolist()
    column_labels = ("timestamp", "hour", "day", "weekOfYear", "month", "year", "weekday")

    NewDict = {}
    for i in column_labels:
        NewDict[i]= [] 
    x=0
    for key in NewDict:
        for i in range(len(time_data)):
            NewDict[key].append((time_data[i][x]) )
        x= x+1

    time_df = pd.DataFrame(NewDict)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        songplay_data = (row.t, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Retrieving each file path to be further processed by the related function.

            Parameters:
                    cur (cursor): A connection cursor
                    conn (Connection): The connection to the dstabase
                    filepath (String): ilepath (String): A string containing the path to the file
                    func (Function): The function each type of file will be processed with

            Returns:
                    None
    '''
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