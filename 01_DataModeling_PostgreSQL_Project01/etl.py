import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Purpose of this method : Create a dataframe using songs log file .Then choses wanted columns and insert in to song table and arist table
    
    Parameters 
    ------------------
    cur : Postgres SQL Cursor object which should be use spakify database.
    filepath : Filepath of the songs logs file
    ------------------
    """
    # open song file
    df = pd.read_json(path_or_buf=filepath,lines=True)

    for index, row in df.iterrows():
        
        # insert song record
        song_data = (row.song_id,row.title,row.artist_id,row.year,row.duration)
        try: 
            cur.execute(song_table_insert, song_data)
        except psycopg2.Error as e: 
            print("Error: Cann't Insert song_data into song_table")
            print (e)
        
    
        # insert artist record
        artist_data = (row.artist_id,row.artist_name,row.artist_location,row.artist_latitude,row.artist_longitude)
        try: 
            cur.execute(artist_table_insert, artist_data)
        except psycopg2.Error as e: 
            print("Error: Cann't Insert artist_data into artist_table")
            print (e)
        
        


def process_log_file(cur, filepath):
    """
    Purpose of this method : Create a dataframe using user event log file .Filter by nextsongs ,Transform timestamp into datetime humanreadble column.Then choses wanted columns and insert in to time_table ,user_table and songsplay_table
    
    Parameters 
    ------------------
    cur : Postgres SQL Cursor object which should be use spakify database.
    filepath : Filepath of the user event logs file
    ------------------
    """
    
    # open log file
    df = pd.read_json(path_or_buf=filepath,lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],utc=True,unit='ms')
    
    # insert time data records
    time_data = []
    column_labels = ["start_time","hour",'day','week_of_year','month','year','weekday']
    
    for rec in t:
        time_data.append([rec,rec.hour,rec.day,rec.week,rec.month,rec.year,rec.day_name()])
    
    time_df = pd.DataFrame.from_records(time_data,columns=column_labels)
    

    for i, row in time_df.iterrows():
        try: 
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e: 
            print("Error: Cann't Insert time_data into time_table")
            print (e)
        

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        try: 
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e: 
            print("Error: Cann't Insert User into user_table")
            print (e)
        

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except psycopg2.Error as e: 
            print("Error: Cann't Join snogs_id or artist id")
            print (e)
        
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.to_datetime(row.ts, utc=True,unit='ms')
        songplay_data =(index, start_time,int(row.userId) , row.level, songid, artistid, row.sessionId, row.location, row.userAgent) 
        try: 
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e: 
            print("Error: Cann't Insert songplay_data into songplay_table")
            print (e)
        


def process_data(cur, conn, filepath, func):
    """
    Purpose of this method : Reads Songs or User activity logfiles given directory and their nested dirrecoty also.       then excute the given function
    
    Parameters 
    ------------------
    cur : Cursor of the sparkifydb database  | Should be psycopg2.cursor()
    conn : Connectio to the sparkifycdb database |Should be psycopg2.connect()
    filepath : Filepath parent of the logs to be analyzed | Should be String
    func : Function to be used to process each log | Should be python function
    ------------------
    
    This function  print Number of process files vs Number of All files 
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
    """
    etl.py start excution form this method. connect to PostgreSql sparkifydb and create connection object.then read       and transform all log files.Then put it into fact and dimentnal tables .   
    """
    # connect to postgre
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the Database")
        print(e)

    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()