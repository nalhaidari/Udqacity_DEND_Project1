# <div align ="centre"> Project 1 </div>


## 1. Introduction.

in this project Sparkfy data engineering team aims to built its own relational database. It had its data in JSON files and the aim of the project is design a database schema as 3NF star schema which would increase data integrity and reduce data redundancy.

### 1.1. data sorce.

the data used is stored in data directory which has 2 main directories one for the logs and the other for the songs.

#### 1.1.1. Log data.

The log_data directory has in it a 2018 logs which has November directory. November directory has 30 files represent the days of the month.
each file has log documents which consisted of the following keys:
['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'method', 'page', 'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']


#### 1.1.1. Song data.

The song directory has has a 3 levels tree of folders represent the the first 3 letters in the song ID. each file has the following key:
['num_songs', 'artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name', 'song_id', 'title', 'duration', 'year']

### 1.2 project files
----
* create_tables.py

    A file containes 4 functions deals with the schema of the data base which are:
 * Create_database
 * create_tables
 * drop_tables
 * main
----

* etl.ipynb

    A Note book to build the workflow of the etl it helped in orgnizing the code and test the code as you write it.
----

* test.ipynb

    A notebook used to insure the expected outcome are met.
---
* etl.py

    A python file has 4 functions which are handling inserting the records into the tables of the database. the functions are:

 * main
 * process_data
 * process_log_file
 * process_song_file
---

* sql_queries.py

    A python file has all the queries to be used by the functions in `etl.py` and `create_tables.py`

---
you can find the code and docstring of every function


## 2. Process.
the process of that project followed designed Database, creating database, creating tables injecting data into tables and finally, test if the database followed the planned architecture

### 2.1. designed Database.

The design of the data base is including user, song, artist, time and songplay tables the first 4 tables can be considered as dimension tables and songplay is a fact table

#### 2.1.1. songplay

Songplay is a table has dummy song played records it presents the time when the song is played, the user, song, artist, session, location and how does the user accessed the web_page. it can be considered as the fact table on our schema. the whole columns of the table described by the following table.

<center>

| Column         | Type          |
|------------    |-----------    |
|songplay_id     |`SERIAL (PK)`  |
| ts             | timestamp(PK) |
| user_id        | VARCHAR       |
| level          | VARCHAR       |
| song_id        | VARCHAR       |
| artist_id      | VARCHAR       |
| session_ID     | int           |
| location       | VARCHAR       |
| user_agent     | VARCHAR       |

</center>

##### 2.1.1.1. Table Creation Query
```
Create table if not exists songplays (songplay_id SERIAL PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id int NOT NULL, level VARCHAR, song_id VARCHAR,
artist_id VARCHAR , session_ID int, location VARCHAR, user_agent VARCHAR);
```

##### 2.1.1.2. Record Insert query
```
Insert into songplays (start_time, user_id, level , song_id,artist_id , session_ID , location , user_agent )
values (%s,%s,%s,%s,%s,%s,%s,%s);
```
##### 2.1.1.3. Table drop query
```
Drop table if exists songplays ;
```


#### 2.1.2. User Table

User table has all the users of Sparkfy. The whole columns of the table described by the following table.  

<center>

| Column        | Type        |
|-----------    |---------    |
| userId        |`VARCHAR (PK)`|
| firstName     | VARCHAR     |
| lastName      | VARCHAR     |
| gender        | VARCHAR     |
| level         | VARCHAR     |

</center>

##### 2.1.2.1. Table Creation Query
```
Create table if not exists users
(userId  int PRIMARY KEY Not NULL,firstName VARCHAR,lastName VARCHAR,
     gender VARCHAR,level VARCHAR);
```

##### 2.1.2.2. Record Insert query
```
INSERT INTO users (userId ,firstName ,lastName , gender ,level) values (%s,%s,%s,%s,%s)
ON CONFLICT (userId) DO
UPDATE
SET level = EXCLUDED.level;

```
##### 2.1.2.3. Table drop query
```
Drop table if exists users ;
```

#### 2.1.3. Song Table


Song Table is keeping the songs records in our database. The whole columns of the table described by the following table.
<center>

| Column       | Type        |
|----------    |---------    |
| song_id      | `VARCHAR (PK)`|
| title        | VARCHAR     |
| artist_ID    | VARCHAR     |
| year         | int         |
| duration     | float       |

</center>

##### 2.1.3.1. Table Creation Query
```
Create table if not exists songs (song_id VARCHAR PRIMARY KEY Not NULL , title VARCHAR,  artist_id VARCHAR,year int ,duration numeric)
```

##### 2.1.3.2. Record Insert query
```
Insert into songs (song_id, title, artist_id , year, duration) values (%s,%s,%s,%s,%s)
ON CONFLICT (song_id) DO NOTHING;
```

with help of song_select query which query the song_id , artist_id by looking for song tile in the songs table and artist_name in the artist table

```
with s as(select song_id from songs where title = %s and duration = %s limit 1)
,a as (select artist_id from artists where  artist_name = %s limit 1)
select s.song_id , a.artist_id from s cross join a;
```

##### 2.1.3.3. Table drop query
```
Drop table if exists songs ;
```

#### 2.1.4. Artist Table

Artist table is keeping artists records in our data base. The whole columns of the table described by the following table.

<center>

| Column               | Type        |
|------------------    |---------    |
| artist_id            | `VARCHAR (PK)`|
| artist_latitude      | float       |
| artist_location      | VARCHAR     |
| artist_longitude     | float       |
| artist_name          | VARCHAR     |

</center>

##### 2.1.4.1. Table Creation Query
```
Create table if not exists artists
( artist_id VARCHAR PRIMARY KEY Not NULL , artist_latitude numeric,
     artist_location VARCHAR ,artist_longitude numeric, artist_name VARCHAR)
```

##### 2.1.4.2. Record Insert query
```
Insert into artists (artist_id, artist_latitude, artist_location,
     artist_longitude, artist_name) values (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) DO NOTHING
```
##### 2.1.4.3. Table drop query
```
Drop table if exists artists ;
```

#### 2.1.5. Time Table

Time table is table storing time elements of the songplay instances to reduce the amount of time to query those elements for analysis purposes.
The whole columns of the table described by the following table.

<center>

| Column           | Type          |
|--------------    |-----------    |
| start_time       | `bigint (PK)` |
| timestamp        | timestamp     |
| hour             | int           |
| day              | int           |
| week_of_year     | int           |
| month            | int           |
| year             | int           |
| weekday          | int           |

</center>

##### 2.1.5.1. Table Creation Query
```
 Create table if not exists time (start_time bigint primary key not
      null,time_stamp timestamp,hour int, day int, week_of_year int,
      month text, year int, weekday VARCHAR)
```

##### 2.1.5.2. Record Insert query
```
Insert into time
(start_time, time_stamp,hour,day,week_of_year
    ,month,year,weekday)
    values (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT
(start_time) DO NOTHING
```
##### 2.1.5.3. Table drop query
```
Drop table if exists time ;
```

### 2.2. creating database.
I used local database server on virtual machine given by Udacity for the sake of accomplishing this project. I used relational database specifically  Postgress database for this project

### 2.3. creating tables and adding the data into the database.
creating the tables and inserting the records performed by using psycopg2 which is a python library adapter for Postgress databases.

### 2.4. ETL.py Functions

#### 2.4.1  main


```
def main():


    """
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
```

#### 2.4.2. process_data

```
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

```
#### 2.4.3. process_log_file

```
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

```
#### 2.4.4. process_song_file

```
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
```

### 2.5. Creating_tables.py

#### 2.4.4. create_database
```
def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn
```
#### 2.4.4. drop_tables
```

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

```
#### 2.4.4. create_tables
```

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

```
#### 2.4.4. main
```

def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.  

    - Drops all the tables.  

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    cur, conn = create_database()
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
```

### 2.6. test.
Test was performed which was provided by the customer and all test statements are passed. additional test were performed which are:


- [x] Check if have only one record with an artist_id in songplays table  
```
%sql SELECT * FROM songplays where artist_id is not null;
```

- [x]  Count of records in time table = 6813
```
%sql select count(*) from time; #6813
```
- [x]  Count of records in artist table = 69
```
%sql select count(*) from artists; #69
```

- [x]  Count of records in songs table = 71
```
%sql select count(*) from songs; # 71
```
- [x]  Count of records in users table = 96
```
%sql select count(*) from users; # 96
```
- [x]  Count of records in songplay table = 6820
```
%sql select count(*) from songplays; # 6820
```
