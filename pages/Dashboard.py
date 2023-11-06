import streamlit as st
from sqlalchemy import create_engine
import pandas as pd

from config import SQL_USERNAME, SQL_PASSWORD, SQL_HOST, SQL_DATABASE

engine = create_engine(f'postgresql+psycopg2://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DATABASE}')

#1:Names of all videos and their corresponding channels
query1 = """
SELECT v.video_name, c.channel_name
FROM video v
JOIN playlist p ON v.playlist_id = p.playlist_id
JOIN channel c ON p.channel_id = c.channel_id
"""
result1 = pd.read_sql(query1, engine)

#2:Channels with the most number of videos and their count
query2 = """
SELECT c.channel_name, COUNT(v.video_id) AS video_count
FROM channel c
JOIN playlist p ON c.channel_id = p.channel_id
JOIN video v ON p.playlist_id = v.playlist_id
GROUP BY c.channel_name
ORDER BY video_count DESC
"""
result2 = pd.read_sql(query2, engine)

#3.Top 10 most viewed videos and their respective channels
query3 = """
SELECT v.video_name, c.channel_name, v.view_count
FROM video v
JOIN playlist p ON v.playlist_id = p.playlist_id
JOIN channel c ON p.channel_id = c.channel_id
ORDER BY v.view_count DESC
LIMIT 10
"""
result3 = pd.read_sql(query3, engine)
#4.Number of comments were made on each video, and their corresponding video names
query4 = """
SELECT v.video_name, COUNT(c.comment_id) AS comment_count
FROM video v
LEFT JOIN comment c ON v.video_id = c.video_id
GROUP BY v.video_name
"""
result4 = pd.read_sql(query4, engine)
#5.Videos with highest number of likes, and their corresponding channel names
query5 = """
SELECT v.video_name, c.channel_name, v.like_count
FROM video v
JOIN playlist p ON v.playlist_id = p.playlist_id
JOIN channel c ON p.channel_id = c.channel_id
ORDER BY v.like_count DESC
LIMIT 1
"""
result5 = pd.read_sql(query5, engine)
#6.Total number of likes for each video, and their corresponding video names
query6 = """
SELECT v.video_name, SUM(v.like_count) AS total_likes
FROM video v
GROUP BY v.video_name
"""
result6 = pd.read_sql(query6, engine)
#7.Total number of views for each channel, and their corresponding channel names
query7 = """
SELECT c.channel_name, SUM(v.view_count) AS total_views
FROM channel c
JOIN playlist p ON c.channel_id = p.channel_id
JOIN video v ON p.playlist_id = v.playlist_id
GROUP BY c.channel_name
"""
result7 = pd.read_sql(query7, engine)
#8.Names of all the channels that have published videos in the year2022
#8.Names of all the channels that have published videos in the year2022
query8 = """
SELECT DISTINCT c.channel_name
FROM channel c
JOIN playlist p ON c.channel_id = p.channel_id
JOIN video v ON p.playlist_id = v.playlist_id
WHERE EXTRACT(YEAR FROM CAST(v.published_date AS DATE)) = 2022;
"""
result8 = pd.read_sql(query8, engine)
#9.Average duration of all videos in each channel, and their corresponding channel names
query9 = """
SELECT c.channel_name, 
       AVG(EXTRACT(EPOCH FROM v.duration::interval)) AS average_duration_seconds
FROM channel c
JOIN playlist p ON c.channel_id = p.channel_id
JOIN video v ON p.playlist_id = v.playlist_id
GROUP BY c.channel_name;
"""
result9 = pd.read_sql(query9, engine)

#10.Videos with highest number of comments, and their corresponding channel names
query10 = """
SELECT v.video_name, c.channel_name, MAX(v.comment_count) AS max_comments
FROM video v
JOIN playlist p ON v.playlist_id = p.playlist_id
JOIN channel c ON p.channel_id = c.channel_id
GROUP BY c.channel_name, v.video_name
ORDER BY max_comments DESC
LIMIT 1
"""
result10 = pd.read_sql(query10, engine)




# Streamlit UI
st.title("YouTube Data Dashboard")

st.header("1. Names of all videos and their corresponding channels")
st.write(result1)
st.header("2. Channels with the most number of videos and their count")
st.write(result2)


st.header("3.Top 10 most viewed videos and their respective channels")
st.write(result3)
st.header("4.Number of comments were made on each video, and their corresponding video names")
st.write(result4)
st.header("5.Videos with highest number of likes, and their corresponding channel names")
st.write(result5)
st.header("6.Total number of likes for each video, and their corresponding video names")
st.write(result6)
st.header("7.Total number of views for each channel, and their corresponding channel names")
st.write(result7)



st.header("8.Names of all the channels that have published videos in the year2022")
st.write(result8)
st.header("9.Average duration of all videos in each channel, and their corresponding channel names")
st.write(result9)
st.header("10.Videos with highest number of comments, and their corresponding channel names")
st.write(result10)


