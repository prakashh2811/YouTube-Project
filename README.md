# YouTube-Project
Introduction: 
Hello Everyone, this is my first data analysis project where i have planned to do some sql analysis with YouTube Data.
Before getting into this project, we all need to know how youtube works and what are all the data we can get from youtube.
For this project i have planned to get three details from youtube those were Channel details, Video details and finally comment details.
In order to get any Youtube data, we can't simply relay on web scraping libraries like beautiful soup, scrapy, etc, since youtube data can be fetched only by using YouTube API key which was provided by Youtube themselves.

Site Link to get Youtube API key:
LINK : https://console.cloud.google.com/marketplace/product/google/youtube.googleapis.com?project=smart-portfolio-421408

Site link to get youtube details :
LINK: https://developers.google.com/youtube/v3

Platforms used for this Project:
                          1)VS Code editor
                          2)MYSQL Workbench
                          3)Streamlit webpage
                          4)Youtube Platform

Tools that was installed for this Project : 
                         1)vs code editor
                         2)Python
                         3)Pandas
                         4)google-api-python-client
                         5)MYSQL server
                         6)sqlalchemy
                         7)pymsql
                         8)Streamlit
                  
Modules imported for this Project in VS code:
                        1)Import pandas
                        2)import sqlalchemy
                        3)from sqlalchemy import create_engine
                        4)import pymysql
                        5)import streamlit
                        6)import googleapiclient.discovery

Skills required to complete this Project:
                        1)Python(Basics upto functions)
                        2)Pandas library
                        3)Basic Idea about Youtube usage and Youtube data
                        4)Basic knowledge about API(Application Programming Interface)
                        5)Relational database management system and any one SQL flavour like mysql, postgres, etc
                        6)Streamlit 
                        7)VS code editor(BASIC)


Problem statement:
We have 10 SQL problem questions that has to be answered via streamlit library.

Project Walkthrough:
Step 1 : With the help of Youtube API key, fetched channel details, video details and comment details.(Created 3 separate functions in python)
step 2 : With the help of pandas library, stored channel, video, comment details in pandas dataframe.
Step 3: Before transferring the pandas data into mysql server, i have created three tables in mysql server - channel_table, video_table and comment_table in the database - Youtube_database
Step 4: With the help of sqlalchemy library, transferred the pandas dataframe data into mysql server- in this case, pandas data such as channel_df, video_df and comment_df into mysql table - channel_table, video_table and comment_table
Step 5: Since there are 10 sql questions to be answered , i have planned to show those data table in pandas dataframe in streamlit library.
Step 6: Finally i wanted everything to get automated in streamlit, so created a input streamlit function and a button where you can give any youtube channel ID which is unique to one another, so when you execute the store button in streamlit automatically all the three tables in mysql server will get updated.
Step 7: And in order to answer those 10 sql problem questions, i have created a streamlit selectbox and imported all those 10 sql questions and created a streamlit button to answer those questions individually.

