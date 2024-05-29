import streamlit as st
import pandas as pd
import pymysql
import sqlalchemy 
from sqlalchemy import create_engine
import googleapiclient.discovery


API_key = "AIzaSyC9YvQ2bUUQdtr-vS1FZmQH283lg788Yd4"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
                api_service_name, api_version, developerKey=API_key)
    
def get_channel_details(ch_ID):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = ch_ID
        )
    response = request.execute()
    
    for item in response['items']:
        my_data = {
            "channel_id" : item['id'],
            "channel_name" : item['snippet']['title'],
            "channel_description" : item['snippet']['description'],
            "channel_published_date" : item['snippet']['publishedAt'],
            "channel_playlist_id" : item['contentDetails']['relatedPlaylists']['uploads'],
            "channel_subscriber_count" : item['statistics']['subscriberCount'],
            "channel_video_count" : item['statistics']['videoCount'],
            "channel_view_count" : item['statistics']['viewCount']
        }
    return my_data
        

def get_videos_ids(chan_id):
    output = get_channel_details(chan_id)
    playlist_id = output['channel_playlist_id']

    video_id_list = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
        part="snippet",
        playlistId= playlist_id,
        maxResults= 50,
        pageToken = next_page_token
        )
        response = request.execute()

        for items in range(len(response['items'])):
            video_id_list.append(response['items'][items]['snippet']['resourceId']['videoId'])
        
        next_page_token = response.get('nextPageToken')

        if next_page_token is None :
            break

    return video_id_list

      
def get_video_details(videos_ids):
    videos_details_list = []
    for ID in videos_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id = ID
        )
        response = request.execute()
        for items in  response['items']:
            data = {
                "channel_title" : items['snippet']['channelTitle'],
                "video_id" : items['id'],
                "channel_id" : items['snippet']['channelId'],
                "video_title" : items['snippet']['title'],
                "video_description" : items['snippet']['description'],
                "video_thumbnail" : items['snippet']['thumbnails']['default']['url'],
                "video_tags" : items.get('tags'),
                "video_published_date" : items['snippet']['publishedAt'],
                "video_duration" : items['contentDetails']['duration'],
                "video_caption_status" : items['contentDetails']['caption'],
                "video_view_count" : items['statistics']['viewCount'],
                "video_like_count" : items['statistics'].get('likeCount',0),
                "video_favorite_count" : items['statistics']['favoriteCount'],
                "video_comment_count" : items['statistics'].get('commentCount', 0)
            }

            videos_details_list.append(data)
    return videos_details_list


def get_comments_details(videos_ids):
    comment_details_list = []
    try:
        for ID in videos_ids:
            request = youtube.commentThreads().list(
                    part="snippet",
                    maxResults=50,
                    videoId = ID
                )
            response = request.execute()
            for items in response['items']:
                data = {
                    "channel_id" : items['snippet']['topLevelComment']['snippet']['channelId'],
                    "video_id" : items['snippet']['topLevelComment']['snippet']["videoId"],
                    "comment_id" : items['snippet']['topLevelComment']['id'],
                    "comment_text" : items['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_author_name' : items['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "comment_publishedAt" : items['snippet']['topLevelComment']['snippet']['publishedAt']

                    }
                comment_details_list.append(data)

    except:
        pass
    return comment_details_list
    

def insert_channel_details(chan_id):
    host = 'localhost'
    user = 'root'
    password = 'root'
    database = 'youtube_database'
    table_name = 'channel_table'

    engine = create_engine(f"mysql+pymysql://root:root@localhost/youtube_database")

    get_channel_list = []
    channel_details = get_channel_details(chan_id)
    get_channel_list.append(channel_details)
    channel_df = pd.DataFrame(get_channel_list)

    channel_df['channel_published_date'] = pd.to_datetime(channel_df['channel_published_date'])

    channel_df.to_sql(name=table_name,con=engine,if_exists='append',index=False)


try:
    client = pymysql.connect(
    host = 'localhost',
    user = 'root',
    password = 'root',
    database = 'youtube_database')

    cursor = client.cursor()

    query = '''create table video_table(channel_title varchar(255),
                                        video_id varchar(255),
                                        channel_id varchar(255),
                                        video_title varchar(255),
                                        video_description text,
                                        video_thumbnail varchar(255),
                                        video_tags text,
                                        video_published_date datetime,
                                        video_duration int,
                                        video_caption_status varchar(255),
                                        video_view_count int,
                                        video_like_count int,
                                        video_favorite_count int,
                                        video_comment_count int)'''

    cursor.execute(query)

except:
    pass




#setting the primary key for video table

try:

    import pymysql

    client = pymysql.connect(
        host = 'localhost',
        user = 'root',
        password = 'root',
        database = 'youtube_database'
        
    )

    cursor = client.cursor()

    query = '''alter table youtube_database.video_table
                add primary key(video_id)'''

    cursor.execute(query)

except:
    pass



def insert_video_details(chan_id):
    videos_ids = get_videos_ids(chan_id)
    video_details = get_video_details(videos_ids)
    video_df = pd.DataFrame(video_details)

    #converting the date data accoring to sql datatime format with pandas methed
    video_df['video_published_date'] = pd.to_datetime(video_df['video_published_date'])
    video_df['video_duration'] = pd.to_timedelta(video_df['video_duration']).dt.total_seconds()

    #using sql alchemy libray to insert pandas data into mysql server
    import sqlalchemy
    from sqlalchemy import create_engine
    import pandas

    host = 'localhost'  #since mysql server installed in my local machine
    user = 'root'       #user name for mysql server
    password = 'root'   #password for mysql server
    database = 'youtube_database'  #database name
    table_name = 'video_table' #table name inside the youtube_database

    engine = create_engine(f"mysql+pymysql://root:root@localhost/youtube_database")

    #using the pandas method to transfer data into mysql server with necessary arguments
    video_df.to_sql(name=table_name,con=engine,if_exists='append',index= False)


#creating the comment table in the mysql server via pymysql library
try:
    import pymysql

    client = pymysql.connect(
        host= 'localhost',
        user= 'root',
        password= 'root',
        database= 'youtube_database'
    )

    cursor = client.cursor()

    query = '''create table comment_table(channel_id  varchar(255),
                                        video_id varchar(255),
                                        comment_id varchar(255),
                                        comment_text text,
                                        comment_author_name varchar(255),
                                        comment_publishedAt datetime)'''

    cursor.execute(query)

except:
    pass


#creating the primary key for the comment table in mysql server via pysql library

try:
    import pymysql

    client = pymysql.connect(
        host= 'localhost',
        user= 'root',
        password= 'root',
        database= 'youtube_database'
    )

    cursor = client.cursor()

    query = '''alter table youtube_database.comment_table
                add primary key(comment_id)'''
    
    cursor.execute(query)

except:
    pass


def insert_comment_details(chan_id):
    videos_ids = get_videos_ids(chan_id)
    comment_details = get_comments_details(videos_ids)

    import pandas as pd
    comment_df = pd.DataFrame(comment_details)
    comment_df['comment_publishedAt'] = pd.to_datetime(comment_df['comment_publishedAt'])

    import sqlalchemy
    from sqlalchemy import create_engine
    #sqlalchemy parameters
    host = 'localhost'
    user = 'root'
    password = 'root'
    database = 'youtube_database'
    table_name = 'comment_table'

    engine = create_engine(f"mysql+pymysql://root:root@localhost/youtube_database")

    comment_df.to_sql(name = table_name,con = engine,if_exists='append',index= False)


def show_channel_table():
    import pymysql

    client = pymysql.connect(
        host= 'localhost',
        user= 'root',
        password= 'root',
        database= 'youtube_database')
    cursor = client.cursor()
    query = '''select * from channel_table'''
    cursor.execute(query)
    data  = cursor.fetchall()
    columns_names = [columns[0] for columns in cursor.description]
    channel_df = pd.DataFrame(data,columns = columns_names)
    channel_df.index += 1
    return channel_df



def show_video_table():
    import pymysql

    client = pymysql.connect(
        host= 'localhost',
        user= 'root',
        password= 'root',
        database= 'youtube_database')
    cursor = client.cursor()
    query = '''select * from video_table'''
    cursor.execute(query)
    data  = cursor.fetchall()
    columns_names = [columns[0] for columns in cursor.description]
    video_df = pd.DataFrame(data,columns = columns_names)
    video_df.index += 1
    return video_df



def show_comment_table():
    import pymysql

    client = pymysql.connect(
        host= 'localhost',
        user= 'root',
        password= 'root',
        database= 'youtube_database')
    cursor = client.cursor()
    query = '''select * from comment_table'''
    cursor.execute(query)
    data  = cursor.fetchall()
    columns_names = [columns[0] for columns in cursor.description]
    comment_df = pd.DataFrame(data,columns = columns_names)
    comment_df.index += 1
    return comment_df

#streamlit part

st.title(":orange[YouTube Data Harvesting Project]")
ID = st.text_input('Enter Channel ID')
if st.button("Store the Data in database"):
    channel_id_list = []
    client = pymysql.connect(
        host= 'localhost',
        user= 'root',
        password= 'root',
        database= 'youtube_database')
    cursor = client.cursor()
    query = '''select * from channel_table'''
    cursor.execute(query)
    data = cursor.fetchall()
    for rows in data:
        channel_id_list.append(rows[0])
    if ID not in channel_id_list:
        insert_channel_details(ID)
        insert_video_details(ID)
        insert_comment_details(ID)
        st.success('channel_table,video_table,comment_table successfully updated')
    else:
        st.success('channel_ID already exist in database')
    client.close()


if st.checkbox("Show Channel Table"):
   st.dataframe(show_channel_table())
if st.checkbox("Show Video Table"):
   st.dataframe(show_video_table())
if st.checkbox("Show Comment Table"):
   st.dataframe(show_comment_table())

sql_questions = st.selectbox("Choose any one SQL question:",["--None--","what are all the names of the videos and their corresponding channels?",
                                                        "which channels have the most number of videos and how many videos do  they have?",
                                                        "what are the top 10 most viewed videos and their respective  channels?",
                                                        "how many comments were made on each video and what are their corresponding video names?",
                                                        "which videos have  the highest no of likes and what are their corresponding channel names?",
                                                        "what is the total number of likes for each video and what are their corresponding video names?",
                                                        "what is the total view count of each channel and what are their corresponding channel names?",
                                                        "what are the names of all the channels that has published videos in the year 2022?",
                                                        "what is the average duration of all videos in each channel and what are their corresponding  channel names?",
                                                        "which  videos have the highest comment count and what are their corresponding channel names?"
                                                        ])

if st.button("Answer"):
    if sql_questions == "--None--":
        pass
    elif sql_questions == "what are all the names of the videos and their corresponding channels?":
        client = pymysql.connect(
            host= 'localhost',
            user = 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = ''' select
                        channel_title,
                        video_title
                    from 
                        video_table'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "which channels have the most number of videos and how many videos do  they have?":
        client = pymysql.connect(
            host= 'localhost',
            user = 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = '''select 
                    channel_name,
                    max(channel_video_count) as highest_video_count
                from 
                    channel_table
                group by 
                    channel_name
                order by 
                    max(channel_video_count) desc'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "what are the top 10 most viewed videos and their respective  channels?":
        client = pymysql.connect(
            host= 'localhost',
            user = 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = '''select 
                    channel_title,
                    video_title,
                    max(video_view_count) as highest_video_view_count
                from 
                    video_table
                group by
                    channel_title,
                    video_title
                order by
                    max(video_view_count) desc
                limit 10'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "how many comments were made on each video and what are their corresponding video names?":
        client = pymysql.connect(
            host= 'localhost',
            user = 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = '''select 
                        video_id,
                        video_title,
                        count(video_comment_count) as each_video_comment_count
                    from 
                        video_table
                    group by 
                        video_id'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "which videos have  the highest no of likes and what are their corresponding channel names?":
        client = pymysql.connect(
            host= 'localhost',
            user = 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = ''' select 
                        channel_title,
                        video_title,
                        max(video_like_count) as highest_video_like_count
                    from 
                        video_table
                    group by
                        channel_title,
                        video_title
                    order by
                        max(video_like_count) desc'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "what is the total number of likes for each video and what are their corresponding video names?":
        client = pymysql.connect(
            host= 'localhost',
            user= 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = '''select 
                        channel_title,
                        video_title,
                        max(video_like_count) as max_video_like_count
                    from 
                        video_table
                    group by
                        channel_title,
                        video_title
                    order by
                        max(video_like_count) desc'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "what is the total view count of each channel and what are their corresponding channel names?":
        client = pymysql.connect(
            host= 'localhost',
            user= 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = ''' select 
                        channel_name,
                        max(channel_view_count) as max_video_count_order
                    from 
                        channel_table
                    group by
                        channel_name
                    order by
                        max(channel_view_count)  desc'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "what are the names of all the channels that has published videos in the year 2022?":
        client = pymysql.connect(
            host= 'localhost',
            user= 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = ''' select 
                        channel_title,
                        video_title,
                        video_published_date
                    from 
                        video_table'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        output_df = pd.DataFrame(data,columns= column_names)
        output_df['video_published_date'] = pd.to_datetime(output_df['video_published_date'])
        output_df['year'] = output_df['video_published_date'].dt.year 
        output_df = output_df.loc[output_df['year'] == 2022 ]
        output_df.reset_index(inplace= True)
        output_df.drop(['year','index'],axis= 1,inplace=True)
        output_df.index += 1
        st.dataframe(output_df)
    elif sql_questions == "what is the average duration of all videos in each channel and what are their corresponding  channel names?":
        client = pymysql.connect(
            host= 'localhost',
            user= 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = ''' select 
                        channel_title,
                        avg(video_duration) as average_video_duration_per_channel
                    from 
                        video_table
                    group by
                        channel_title
                    order by
                        avg(video_duration) desc'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns=column_names)
        df.index += 1
        st.dataframe(df)
    elif sql_questions == "which  videos have the highest comment count and what are their corresponding channel names?":
        client = pymysql.connect(
            host= 'localhost',
            user= 'root',
            password= 'root',
            database= 'youtube_database')
        cursor = client.cursor()
        query = ''' select 
	                    video_title,
                        channel_title,
                        max(video_comment_count) as max_video_comment_count
                    from 
	                    video_table
                    group by
	                    channel_title,
                        video_title
                    order by
	                    max(video_comment_count) desc'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [columns[0] for columns in cursor.description]
        df = pd.DataFrame(data,columns= column_names)
        df.index += 1
        st.dataframe(df)

with st.sidebar:
    st.title(":red[Project Info]")
    st.header(":yellow[Skills used in this project:]")
    st.caption("Python")
    st.caption('Pandas')
    st.caption('Mysql server')
    st.caption('Streamlit')
    st.header("Libraries imported in this project")
    st.caption("pandas")
    st.caption("pymysql")
    st.caption('sqlalchemy')
    st.caption("streamlit")
    st.header("Tools used in this project")
    st.caption("VS Code Editor")
    st.caption("Mysql workbench")
    st.caption("Streamlit")
