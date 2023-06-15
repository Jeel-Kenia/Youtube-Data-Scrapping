import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime
import pymongo
import psycopg2
from psycopg2 import Error
import matplotlib.pyplot as plt 
import time
 
#Get the API key, service and version
api_key = 'Enter the API Key taken from Google Console'
api_service_name = "youtube"
api_version = "v3"
# Get credentials and create an API client
youtube = build(api_service_name, api_version, developerKey=api_key)

# The channel stats function to get channel details from youtube
def get_channel_stats(youtube, channel_id):
    all_data = {}

    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    )
    response = request.execute()

    for item in response['items']:
        playlist_id = item['contentDetails']['relatedPlaylists']['uploads']
        video_ids = get_video_ids(youtube, playlist_id)

        data = {
            'channel_name': item['snippet']['title'],
            'channel_id': item['id'],
            'subscribers': item['statistics']['subscriberCount'],
            'views': item['statistics']['viewCount'],
            'channel_description': item['snippet'].get('description'),
            'total_videos': item['statistics']['videoCount'],
            'playlist_id': playlist_id,
            'video_id': get_video_details(youtube, video_ids)
        }
        all_data.update(data)

    return all_data

# The video IDs function to get video ids corresponding to playlist id
def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    video_ids = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

    return video_ids

# The video details function to get all the video details
def get_video_details(youtube, video_ids):
    all_video_stats = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response = request.execute()

        for item in response['items']:
            video_stats = {
                'video_id': video_id,
                'title': item['snippet']['title'],
                'video_description': item['snippet']['description'],
                'published_at': item['snippet']['publishedAt'],
                'views': item['statistics']['viewCount'],
                'likes': item['statistics']['likeCount'],
                'duration': item['contentDetails']['duration'],
                'thumbnails': item['snippet']['thumbnails']['default']['url'],
                'caption_status': item['contentDetails']['caption'],
                'comments': get_comment_videoinfo(youtube, video_id)
            }
            all_video_stats.append(video_stats)

    return all_video_stats

# The comment details function to get all comment details related to a particular video id
def get_comment_videoinfo(youtube, video_id):
    all_comments_stats = []
    comments_stats = []

    try:
        request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id
        )
        video_response = request.execute()
        for comment in video_response['items']:
            comments_stats.append(dict(
                comment_id = comment['snippet']['topLevelComment']['id'],
                comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_authorc= comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']
            ))
    except HttpError as e:
        if e.resp.status == 403:
            pass
            #print(f"Comments are disabled for video {video_id}")

    all_comments_stats.extend(comments_stats)
    return all_comments_stats

#Function for pushing data into MongoDB
def push_to_mongodb(data):
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://guvi2023:<'your_password'>-x3adhoo-shard-00-00.wgfrtva.mongodb.net:27017,ac-x3adhoo-shard-00-01.wgfrtva.mongodb.net:27017,ac-x3adhoo-shard-00-02.wgfrtva.mongodb.net:27017/?ssl=true&replicaSet=atlas-12iwpy-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = client['Youtube']
        collection = db['Data']

        # Insert data into MongoDB
        collection.insert_one(data)

        # Close the MongoDB connection
        client.close()

        st.success('Data pushed to MongoDB successfully!')
    except Exception as e:
        st.error(f'Error occurred while pushing data to MongoDB: {str(e)}')

def mongo_connect():
    client = pymongo.MongoClient("mongodb://guvi2023:RqIFSKy96frpVcMd@ac-x3adhoo-shard-00-00.wgfrtva.mongodb.net:27017,ac-x3adhoo-shard-00-01.wgfrtva.mongodb.net:27017,ac-x3adhoo-shard-00-02.wgfrtva.mongodb.net:27017/?ssl=true&replicaSet=atlas-12iwpy-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = client['Youtube']
    collection = db['Data']
    return collection

#Function to get the list of channel names
def channels_list(): 
    coll = mongo_connect()
    cur,connection = sql_connection()
    channel_list = [i['channel_name'] for i in coll.find({})]
    
    # SQL code to fetch channel_name and channel_id from channel_details table
    qry = "SELECT channel_name FROM channel_data"
    cur.execute(qry)
    sql_channel = [row[0] for row in cur.fetchall()]
        
    cur.close()
    connection.close()

    #sqlcode 
    new_channel_list = [channel for channel in channel_list if channel not in sql_channel]
    return new_channel_list       

#Function to convert duration into seconds
def duration_to_seconds(duration):
    duration = duration[2:]  # Remove the 'PT' prefix
    seconds = 0

    # Check for hours
    if 'H' in duration:
        hours, duration = duration.split('H')
        seconds += int(hours) * 3600

    # Check for minutes
    if 'M' in duration:
        minutes, duration = duration.split('M')
        seconds += int(minutes) * 60

    # Check for seconds
    if 'S' in duration:
        seconds = duration.split('S')[0]

    return int(seconds)

#Function for connecting to PostgreSQL server
def sql_connection():
    try:
        connection = psycopg2.connect(
            host='Host_name', 
            user ='user_name',
            password='password',
            port=port_number,
            database='database_name'
        )
        cur = connection.cursor()
    except Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    return cur,connection

#Function for creating table
def create_table():
    cur,connection = sql_connection()
    cur.execute("CREATE TABLE IF NOT EXISTS channel_data(channel_name varchar(50),channel_id varchar(50) PRIMARY KEY,views int,channel_description text)")
    cur.execute("CREATE TABLE IF NOT EXISTS playlist_data(playlist_id varchar(50) PRIMARY KEY, channel_id varchar(50) REFERENCES channel_data(channel_id), channel_name varchar(50))")
    cur.execute("CREATE TABLE IF NOT EXISTS video_data(video_id varchar(50) PRIMARY KEY, playlist_id varchar(50), title varchar(100), description text, published_at timestamp, view_count int, like_count int, duration int, thumbnail varchar(100), caption_status varchar(50),FOREIGN KEY(playlist_id) REFERENCES playlist_data(playlist_id))")
    cur.execute("CREATE TABLE IF NOT EXISTS comment_data(video_id varchar(50) REFERENCES video_data(video_id), comment_id varchar(50) PRIMARY KEY, comment_author varchar(50), comment_text text, comment_published_at timestamp)")
    connection.commit()
    cur.close()
    connection.close()

#function to get dataframe of channel details
def chann_details():
    collection = mongo_connect()
    data1 = collection.find({}, {
        'channel_id': 1,
        'channel_name': 1,
        'views': 1,
        'channel_description': 1,
        '_id': 0
    })
    df1 = pd.DataFrame(data1)
    # st.write(df1.dtypes)  # Print the DataFrame
    return df1

#Function to push channel details to sql channel data table
def chann_details_sql(mongo_data):
    cur,connection = sql_connection()
    
    try:
        # cur = connection.cursor()
        # st.write(chann_details())
        # Convert the DataFrame to a list of tuples
        rows = [tuple(row) for row in chann_details().to_numpy()]
        # st.write(rows)
        # Create the INSERT statement with placeholders for the columns
        insert_query = "INSERT INTO channel_data (channel_name, channel_id, views, channel_description) VALUES (%s, %s, %s, %s)"
        chann_id = []
        
        for row in rows:
            if mongo_data == row[0]:
                # st.write(row[0])
                cur.execute(insert_query, row)
                chann_id = row[1]

        connection.commit()
        print("Data imported successfully!")
    except Error as e:
        print(f"Error importing data: {e}")
    finally:
        cur.close()
        connection.close()
        return chann_id

#function to get dataframe of playlist details
def playlist_details():
    collection = mongo_connect()
    data2 = collection.find({}, {
    'channel_name': 1,
    'channel_id': 1,
    'playlist_id': 1,
    '_id': 0
})
    df2 = pd.DataFrame(data2)
    # st.write("jdsvjsdvsk", df2.dtypes)
    return(df2)

#Function to push playlist details to sql playlist data table
def playlist_details_sql(channelid1):
    cur, connection = sql_connection()
    try:
        rows2 = [tuple(row) for row in playlist_details().to_numpy()]
        insert_query2 = "INSERT INTO playlist_data(channel_name, channel_id, playlist_id) VALUES (%s, %s, %s)"
        playlst_id = []
        for row in rows2:
            try:
                if channelid1 == row[1]:
                    cur.execute(insert_query2, row)
                    playlst_id = row[2]
            except Error as e:
                print(f"Error executing SQL query: {e}")
        
        connection.commit()
        print("Data imported successfully!")
    except Error as e:
        print(f"Error fetching data from database: {e}")
    finally:
        cur.close()
        connection.close()
    
    return playlst_id

#function to get dataframe of video details
def video_details():
    
    video_ids = []
    playlist_ids = []
    video_titles = []
    video_descriptions = []
    published_dates = []
    view_counts = []
    like_counts = []
    durations = []
    thumbnails = []
    caption_statuses = []

    for data in mongo_connect().find():
        for video in data['video_id']:
            video_ids.append(video['video_id'])
            playlist_ids.append(data['playlist_id'])
            video_titles.append(video['title'])
            video_descriptions.append(video['video_description'])
            published_dates.append(video['published_at'])
            view_counts.append(video['views'])
            like_counts.append(video['likes'])
            durations.append(duration_to_seconds(video['duration']))
            thumbnails.append(video['thumbnails'])
            caption_statuses.append(video['caption_status'])

        # Create a DataFrame from the extracted data
        df4 = pd.DataFrame({
            'video_id': video_ids,
            'playlist_id': playlist_ids,
            'title': video_titles,
            'description': video_descriptions,
            'published_at': published_dates,
            'view_count': view_counts,
            'like_count': like_counts,
            'duration': durations,
            'thumbnail': thumbnails,
            'caption_status': caption_statuses
        })
    return(df4)

#Function to push video details to sql video data table
def video_details_sql(playlistid):
    cur,connection = sql_connection()
    try:
        # cur = connection.cursor()
        rows4 = [tuple(row) for row in video_details().to_numpy()]
        insert_query4 = "INSERT INTO video_data (video_id, playlist_id, title, description, published_at, view_count, like_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        vid_id = []
        for i,row in enumerate(rows4):
            if playlistid == row[1]:
                cur.execute(insert_query4, row)
                vid_id.append(row[0])
        connection.commit()
        print("Data imported successfully!")
    except Error as e:
        print(f"Error importing data: {e}")
    finally:
        cur.close()
        connection.close()
    return vid_id

#function to get dataframe of comment details
def comment_details(mongo_data):
    video_ids = []
    comment_ids = []
    comment_authors = []
    comment_texts = []
    comment_publishedDates = []
    
    qry = {'channel_name': mongo_data}
    for data in mongo_connect().find(qry):
        for video in data['video_id']:
            video_id = video['video_id']
            for comment in video['comments']:
                comment_id = comment['comment_id']
                comment_author = comment['comment_authorc']
                comment_text = comment['comment_text']
                comment_publishedAt = comment['comment_published_at']
                video_ids.append(video_id)
                comment_ids.append(comment_id)
                comment_authors.append(comment_author)
                comment_texts.append(comment_text)
                comment_publishedDates.append(comment_publishedAt)

        # Create a DataFrame from the extracted data
        df3 = pd.DataFrame({
            'video_id': video_ids,
            'comment_id': comment_ids,
            'comment_author': comment_authors,
            'comment_text': comment_texts,
            'comment_published_at': comment_publishedDates
        })
        return df3
        
#Function to push comment details to sql comment data table
def comment_details_sql(videoid,mongo_data):
    cur,connection = sql_connection()
    try:
        cur = connection.cursor()
        rows3 = [tuple(row) for row in comment_details(mongo_data).to_numpy()]
        insert_query3 = "INSERT INTO comment_data (video_id, comment_id, comment_author, comment_text, comment_published_at) VALUES (%s, %s, %s, %s, %s)"
        for i,row in enumerate(rows3):
            if row[0] in videoid: # VI[1,2,3,4,5] == 3
                cur.execute(insert_query3, row)
        

        connection.commit()
        print("Data imported successfully!")
    except Error as e:
        print(f"Error importing data: {e}")
    finally:
        cur.close()
        connection.close()

##Functions for running sql queries for data analysis

def get_video_channel_data():
    cur, connection = sql_connection()
    try:
        cur.execute("SELECT video_data.title, playlist_data.channel_name FROM video_data JOIN playlist_data ON video_data.playlist_id = playlist_data.playlist_id")
        result = cur.fetchall()
        # Extract column names from the cursor description
        columns = [desc[0] for desc in cur.description]
        
        # Create a list to hold the data rows
        data_rows = []
        
        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving video channel data: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_channel_video_count():
    cur, connection = sql_connection()
    try:
        cur.execute("""
            SELECT playlist_data.channel_name, COUNT(video_data.video_id) AS video_count
            FROM playlist_data
            JOIN video_data ON playlist_data.playlist_id = video_data.playlist_id
            GROUP BY playlist_data.channel_name
            ORDER BY video_count DESC
        """)
        result = cur.fetchall()
        
        # Create a list to hold the data rows
        data_rows = []

        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        # Define the column names
        columns = ['Channel Name', 'Video Count']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving channel video count: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_top_10_viewed_videos():
    cur, connection = sql_connection()
    try:
        cur.execute("""
            SELECT video_data.title, playlist_data.channel_name, video_data.view_count
            FROM video_data
            JOIN playlist_data ON playlist_data.playlist_id = video_data.playlist_id
            ORDER BY video_data.view_count DESC
            LIMIT 10
        """)
        result = cur.fetchall()
        # st.write("ssssss",result)
        # Create a list to hold the data rows
        data_rows = []

        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        # Define the column names
        columns = ['Video Title', 'Channel Name', 'View Count']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving channel video count: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_comments_on_video_data():
    cur, connection = sql_connection()
    try:
        cur.execute("""
            SELECT video_data.title, COUNT(comment_data.comment_id) AS comment_count
            FROM video_data
            LEFT JOIN comment_data ON video_data.video_id = comment_data.video_id
            GROUP BY video_data.title
        """)
        result = cur.fetchall()
        # st.write("ssssss",result)
        # Create a list to hold the data rows
        data_rows = []

        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        # Define the column names
        columns = ['Video Title', 'Number of Comments']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving channel video count: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_top_10_liked_videos():
    cur, connection = sql_connection()
    try:
        cur.execute("""
            SELECT video_data.title, playlist_data.channel_name, video_data.like_count
            FROM video_data
            JOIN playlist_data ON playlist_data.playlist_id = video_data.playlist_id
            ORDER BY video_data.like_count DESC
            LIMIT 10
        """)
        result = cur.fetchall()
        # st.write("ssssss",result)
        # Create a list to hold the data rows
        data_rows = []

        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        # Define the column names
        columns = ['Video Title', 'Channel Name', 'Like Count']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving channel video count: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_video_likes_data():
    cur, connection = sql_connection()
    try:
        cur.execute("""
        SELECT video_data.title, video_data.like_count
        FROM video_data 
    """)
        result = cur.fetchall()
          
        # Create a list to hold the data rows
        data_rows = []
        
        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        columns = ['Video Title', 'Like Count']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving video channel data: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_channel_views_data():
    cur, connection = sql_connection()
    try:
        cur.execute("""
        SELECT channel_data.channel_name, channel_data.views
        FROM channel_data 
    """)
        result = cur.fetchall()
          
        # Create a list to hold the data rows
        data_rows = []
        
        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        columns = ['Channel Name', 'Total Views']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving video channel data: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_video_published_data():
    cur, connection = sql_connection()
    try:
        cur.execute("""
        SELECT DISTINCT playlist_data.channel_name, MAX(video_data.published_at) AS latest_published_date
        FROM video_data
        JOIN playlist_data ON video_data.playlist_id = playlist_data.playlist_id
        WHERE EXTRACT(YEAR FROM video_data.published_at) = 2022
        GROUP BY playlist_data.channel_name
    """)
    
        result = cur.fetchall()
          
        # Create a list to hold the data rows
        data_rows = []
        
        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        columns = ['Channel Name', 'Latest Video Published Date']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving video channel data: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_average_video_duration_data():
    cur, connection = sql_connection()
    try:
        cur.execute("""
            SELECT playlist_data.channel_name, ROUND(AVG(video_data.duration),3) AS average_duration
            FROM video_data
            JOIN playlist_data ON playlist_data.playlist_id = video_data.playlist_id
            GROUP BY playlist_data.channel_name
        """)
        result = cur.fetchall()
        # st.write("ssssss",result)
        # Create a list to hold the data rows
        data_rows = []

        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        # Define the column names
        columns = ['Channel Name', 'Average Duration of Videos']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving channel video count: {e}")
    finally:
        cur.close()
        connection.close()
    return st.table

def get_max_comments_data():
    cur, connection = sql_connection()
    try:
        cur.execute("""
            SELECT video_data.title, playlist_data.channel_name, COUNT(comment_data.comment_id) AS comment_count
            FROM video_data
            JOIN playlist_data ON video_data.playlist_id = playlist_data.playlist_id
            JOIN comment_data ON video_data.video_id = comment_data.video_id
            GROUP BY video_data.title, playlist_data.channel_name
            ORDER BY comment_count DESC
            LIMIT 10
        """)
        result = cur.fetchall()
        # st.write("ssssss",result)
        # Create a list to hold the data rows
        data_rows = []

        # Iterate over the result and append each row to the data list
        for row in result:
            data_rows.append(list(row))

        # Define the column names
        columns = ['Video Name','Channel Name','Comment Count']

        # Display the data in a table with column headers
        st.table(pd.DataFrame(data_rows, columns=columns))

    except Error as e:
        print(f"Error retrieving channel video count: {e}")
    finally:
        cur.close()
        connection.close()

    # Extract the video titles, channel names, and comment counts
    video_titles = [row[0] for row in result]
    channel_names = [row[1] for row in result]
    comment_counts = [row[2] for row in result]

    # Plot the bar chart
    plt.bar(video_titles, comment_counts)
    plt.xlabel('Video Title')
    plt.ylabel('Comment Count')
    plt.title('Videos with Highest Number of Comments')
    plt.xticks(rotation=90)
    st.pyplot(plt)


#Function to query the list of questions fr Data Analysis and return the output in tabular format.
def query_data():
    query_list = [None,'1) What are the names of all the videos and their corresponding channels?',
                  '2) Which channels have the most number of videos, and how many videos do they have?',
                  '3) What are the top 10 most viewed videos and their respective channels?',
                  '4) How many comments were made on each video, and what are their corresponding video names?',
                  '5) Which videos have the highest number of likes, and what are their corresponding channel names?',
                  '6) What is the total number of likes for each video, and what are their corresponding video names?',
                  '7) What is the total number of views for each channel, and what are their corresponding channel names?',
                  '8) What are the names of all the channels that have published videos in the year 2022?',
                  '9) What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                  '10) Which videos have the highest number of comments, and what are their corresponding channel names?'
                  ]
    query = st.sidebar.selectbox('Select the Query', query_list)
    # return query
    if query == None:
        pass
    elif query =='1) What are the names of all the videos and their corresponding channels?':
        st.subheader('Names of all the videos and their corresponding channels')
        return get_video_channel_data()
    elif query =='2) Which channels have the most number of videos, and how many videos do they have?':
        st.subheader('Channels with number of published videos')
        return get_channel_video_count()
    elif query =='3) What are the top 10 most viewed videos and their respective channels?':
        st.subheader('Videos with maximum number of views')
        return get_top_10_viewed_videos()
    elif query =='4) How many comments were made on each video, and what are their corresponding video names?':
        st.subheader('Number of Comments on each video')
        return get_comments_on_video_data()
    elif query =='5) Which videos have the highest number of likes, and what are their corresponding channel names?':
        st.subheader('Videos with maximum number of likes')
        return get_top_10_liked_videos()
    elif query =='6) What is the total number of likes for each video, and what are their corresponding video names?':
        st.subheader('Number of Comments on each video')
        return get_video_likes_data()
    elif query == '7) What is the total number of views for each channel, and what are their corresponding channel names?':
        st.subheader('Total Views on each channel')
        return get_channel_views_data()
    elif query == '8) What are the names of all the channels that have published videos in the year 2022?':
        st.subheader('Channels that published videos in 2022')
        return get_video_published_data()
    elif query == '9) What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        st.subheader('Average duration of videos')
        return get_average_video_duration_data()
    elif query == '10) Which videos have the highest number of comments, and what are their corresponding channel names?':
        st.subheader('Videos with maximum comments')
        return get_max_comments_data()
        

##Setup and functions for Streamlit application

#Setting page lavout
st.set_page_config(
    page_title = "Youtube Scrapper",
    layout = "centered",
    page_icon = ":dna:",
    menu_items = {
    'About' : "Created by Jeel Kenia 'https://www.linkedin.com/in/jeel-kenia/'"
    }
    )
    

# Define global variables
get_channl_stats = None
push_mongodb = None
push_sql = None
drop_sql = None
mongo_data = None
channel_id = None

# Define functions for each page
def home_page():
    global get_channl_stats, push_mongodb, push_sql, drop_sql, mongo_data, channel_id
    st.title('YouTube Channel Statistics')
    channel_id = st.text_input('Enter YouTube Channel ID')
    get_channl_stats = st.button('Get Channel Statistics')
    push_to_mongodb = st.sidebar.write('Click below to push data to MongoDB')
    push_mongodb = st.sidebar.button('Push to MongoDB')
    mongo_data = st.sidebar.selectbox('Channel Details', channels_list())
    push_to_sql = st.sidebar.write('Select a channel name and click below to push data to SQL')
    push_sql = st.sidebar.button('Push to SQL')
    push_for_analysis = st.sidebar.write('Click on Analysis and Reports Page after pushing data to SQL!!')
    

def analysis_page():
    st.title('Analysis and Reports')
    st.write('Select the query from the dropdown on the left to fetch results')
    query_data()
    

# Create a dictionary to map page names to their corresponding functions
pages = {
    'Data Retrieval and Processing': home_page,
    'Analysis and Reports': analysis_page,
}

# Add a sidebar to select the page
selected_page = st.sidebar.selectbox('Select Page', list(pages.keys()))

# Run the function corresponding to the selected page
pages[selected_page]()


##Final Code for Streamlit app

if get_channl_stats:
    if channel_id:
        st.text('Fetching channel statistics...')
        data = get_channel_stats(youtube, channel_id)
        st.write('Channel Statistics')
        st.write(data)
        
    else:
        st.warning('Please enter a YouTube Channel ID.')

if push_mongodb:
    if channel_id:
        data = get_channel_stats(youtube, channel_id)
        push_to_mongodb(data)
    
if push_sql:
    create_table()
    channelid = chann_details_sql(mongo_data)
    playlistid = playlist_details_sql(channelid)
    videoid = video_details_sql(playlistid)
    comment_details_sql(videoid,mongo_data)
    st.success('Data pushed to SQL successfully!')
    
