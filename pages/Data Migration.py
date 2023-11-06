from pymongo import MongoClient
import streamlit as st
import pandas as pd
import datetime

from sqlalchemy import create_engine
import re
from config import DB_USERNAME, DB_PASSWORD, DB_NAME, COLLECTION_NAME,SQL_USERNAME, SQL_PASSWORD, SQL_HOST, SQL_DATABASE

def mongodb_connection():

    uri = f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}@cluster0.sb8opju.mongodb.net/?retryWrites=true&w=majority&appName=AtlasApp"

    client = MongoClient(uri)

    try:
        client.admin.command('ping')
        status = "connected!"
    except Exception as e:
        status = f"Connection failed: {e}"

    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    return status, collection

def mysql_connection(video_tb,playlist_tb,comment_tb,channel_tb):

    connection = create_engine(f'postgresql+psycopg2://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DATABASE}')

    existing_channel_ids = set(pd.read_sql_query("SELECT channel_id FROM channel", connection)["channel_id"])

    channel_tb = channel_tb[~channel_tb["channel_id"].isin(existing_channel_ids)]
    if not channel_tb.empty:
        channel_tb.to_sql(
            name="channel",
            con=connection,
            if_exists="append",
            index=False
        )
    existing_playlist_ids = set(pd.read_sql_query("SELECT playlist_id FROM playlist", connection)["playlist_id"])

    playlist_tb = playlist_tb[~playlist_tb["playlist_id"].isin(existing_playlist_ids)]
    if not channel_tb.empty:
        playlist_tb.to_sql(
            name="playlist",
            con=connection,
            if_exists="append",
            index=False
        )

    existing_video_ids = set(pd.read_sql_query("SELECT video_id FROM video", connection)["video_id"])

    # Filter out duplicate rows before inserting
    video_tb = video_tb[~video_tb["video_id"].isin(existing_video_ids)]
    if not channel_tb.empty:
        video_tb.to_sql(
            name="video",
            con=connection,
            if_exists="append",
            index=False
        )

    existing_comment_ids = set(pd.read_sql_query("SELECT comment_id FROM comment", connection)["comment_id"])

    comment_tb = comment_tb[~comment_tb["comment_id"].isin(existing_comment_ids)]
    if not channel_tb.empty:
        comment_tb.to_sql(
            name="comment",
            con=connection,
            if_exists="append",
            index=False
        )
    #connection.close()

    return "Success"

def convert_duration(duration_str):
    hours = minutes = seconds = 0

    hours_match = re.search(r'(\d+)H', duration_str)
    if hours_match:
        hours = int(hours_match.group(1))
    minutes_match = re.search(r'(\d+)M', duration_str)
    if minutes_match:
        minutes = int(minutes_match.group(1))
    seconds_match = re.search(r'(\d+)S', duration_str)
    if seconds_match:
        seconds = int(seconds_match.group(1))

    formatted_duration = f'{hours:02d}:{minutes:02d}:{seconds:02d}'
    return formatted_duration

def data_cleaning(result):
    df = pd.DataFrame(result)
    # channel table
    df['view_count'] = df['view_count'].astype('int')
    channel_tb = df[['channel_id','channel_name','Channel_Type','view_count','description','Channel_Status']]
    channel_tb=channel_tb.rename(columns={'Channel_Type':'channel_type','view_count':'channel_views','description':'channel_description','Channel_Status':'channel_status'})
    # playlist table
    playlist_ids = [col for col in df.columns if col.startswith('playlist_id')]
    playlist_data = df[playlist_ids].transpose()
    playlist_data = playlist_data.reset_index(drop=True)
    playlist_names = [col for col in df.columns if col.startswith('playlist_name')]
    playlist_names = df[playlist_names].transpose()
    playlist_names = playlist_names.reset_index(drop=True)
    playlist_tb = pd.concat([playlist_data, playlist_names], ignore_index=True, axis=1)
    playlist_tb.columns = ['playlist_id', 'playlist_name']
    playlist_tb['channel_id'] = str(df.loc[0, 'channel_id'])
    # comment table
    video_ids = [col for col in df.columns if col.startswith('video_id')]
    video_data = df[video_ids]
    comment_tb = pd.DataFrame(
        columns=['comment_id','video_id', 'comment_text', 'comment_author', 'comment_published_date'])
    for i in video_data:
        dic = df[i]
        dicts = dic.to_dict().pop(0)
        for comment_id, comment_info in dicts["Comments"].items():
            comment_tb.loc[len(comment_tb)] = [comment_info["comment_id"], dicts['video_id'], comment_info["comment_text"], comment_info["Comment_Author"],
                                               comment_info["Comment_PublishedAt"]]
    comment_tb['comment_published_date']= pd.to_datetime(comment_tb['comment_published_date'])
    comment_tb['comment_published_date'] = comment_tb['comment_published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # video table
    playlist_lst = [playlist_tb.loc[(int(s.split('_')[-1])), "playlist_id"] for s in video_ids]
    video_tb = pd.DataFrame(
        columns=['video_id', 'video_name', 'video_description', 'published_date', 'view_count', 'like_count', 'favorite_count',
                 'comment_count', 'duration', 'thumbnail', 'caption_status'])
    for j in video_data:
        v_dic = df[j]
        v_dicts = v_dic.to_dict().pop(0)
        video_tb.loc[len(video_tb)] = [v_dicts['video_id'], v_dicts['title'], v_dicts['description'],
                                       v_dicts['PublishedAt'], v_dicts['View_Count'], v_dicts['likes'],
                                       v_dicts['Favorite_Count'], v_dicts['Comment_Count'],
                                       v_dicts['Duration'], v_dicts['Thumbnail'], v_dicts['Caption_Status']]
    video_tb['playlist_id'] = playlist_lst
    video_tb['published_date'] = pd.to_datetime(video_tb['published_date'])
    video_tb['published_date'] = video_tb['published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    video_tb['duration'] = video_tb['duration'].apply(convert_duration)
    return video_tb,playlist_tb,comment_tb,channel_tb

def main():
    monodb_status, mg_collection = mongodb_connection()
    channel_names = mg_collection.distinct("channel_name")
    selected_channel_name = st.selectbox("Select a channel name", channel_names)
    if st.button("Submit"):
        my_bar = st.progress(0, f"Fetching Data from MongoDB for {selected_channel_name}")
        result = list(mg_collection.find({"channel_name": f"{selected_channel_name}"}))
        my_bar.progress(10, "Data to DataFrame")
        video_tb, playlist_tb, comment_tb, channel_tb= data_cleaning(result)

        my_bar.progress(80, "Inserting Data to Mysql")
        mysql_connection(video_tb, playlist_tb, comment_tb, channel_tb)
        my_bar.progress(100, "Completed")


if __name__ == "__main__":
    main()
