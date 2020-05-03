import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from create_tables import create_tables, create_database


def process_song_file(cur, filepath):
    """
    This function reads song_data file and insert the records into database throgh the provided cursor
    Input:
    - cur: data base connection cursor
    - filepath: a bath of JASON file to get the records from.
    
    output: 
    - None
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id', 'title','artist_id', 'year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_latitude','artist_location','artist_longitude','artist_name']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
     This function reads log_data file and insert the records into database throgh the provided cursor
    Input:
    - cur: database connection cursor
    - filepath: a bath of JASON file to get the records from.
    
    output: 
    - None 
    """
    # open log file
    df = pd.read_json(filepath , lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df.ts,unit='ms')
    
    t = df['ts'].to_frame()
    t['timestamp'] = pd.to_datetime(t.ts,unit='ms')
    t['hour'] = t.timestamp.dt.hour 
    t['day'] = t.timestamp.dt.day
    t['week_of_year'] = t.timestamp.dt.weekofyear
    t['month'] = t.timestamp.dt.month
    t['year'] = t.timestamp.dt.year
    t['weekday'] = t.timestamp.dt.weekday
    
    # insert time data records
    column_labels = ['ts',"timestamp","hour", "day", "week_of_year", "month", "year", "weekday"]
    time_data = t[column_labels].values.tolist()
    time_df = pd.DataFrame({key:[x[i] for x in time_data] for i,key in enumerate(column_labels)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
    #looping through records on the lof file
    
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length , row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.timestamp, row.userId, row.level, songid, artistid, row.sessionId , row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    this function is like an orgnizer of our pipeline it takes connection prameter and main directory and a finction to apply to every file stored in thr directory
    the file search is recersevly so every file stored in all sub-dirs will be processed.
    input
    cur:       Connection cursor.
    conn:      Database connection object.
    filepath:  Main filepath.
    func:      Function to apply
    
    output: None
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
    main function is to drive the whole pipe line of the project. it build the connection the database and then apply process_data twice
    the first process_data is to process songfiles.
    the secound is to process log files.
    and then it colse the connection.
    input: None
    output None
    """
    cur, conn = create_database()
    create_tables(cur,conn)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":  #this statement is run the main function whenever etl.py file was run from terminal and aviod run in case of importation
    main()