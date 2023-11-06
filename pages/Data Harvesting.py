import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import streamlit as st
from config import API_KEY, DB_USERNAME, DB_PASSWORD, DB_NAME, COLLECTION_NAME


api_service_name = "youtube"
api_version = "v3"

def create_youtube_service():
    youtube = build(api_service_name, api_version, developerKey=API_KEY)
    return youtube

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

def yt_channel_id(youtube, channel_name):
    request = youtube.search().list(
        q=channel_name,
        type="channel",
        part="id",
        maxResults=1
    )
    response = request.execute()
    return response["items"][0]["id"]["channelId"]

def yt_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()
    return response

def yt_playlists(youtube, channel_id):
    playlists = []
    playlist_ids = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )

        response = request.execute()
        if "items" in response:
            playlist_ids.extend(item["id"] for item in response["items"])
        playlists.extend(response.get("items", []))
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return playlists,playlist_ids

def yt_video_details(youtube, playlist_id):
    video_ids=[]
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        if "items" in response:
            video_ids.extend([item["contentDetails"]["videoId"] for item in response["items"]])
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return video_ids

def get_video_detail(youtube, video_id):
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id
    )

    response = request.execute()

    return response

def yt_video_comments(youtube, video_id):
    try:
        next_page_token = None
        while True:
            request = youtube.commentThreads().list(
                part="id,snippet",
                videoId=video_id,
                maxResults=50,
                pageToken=next_page_token
            )

            response = request.execute()

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
    except HttpError as e:
        if e.resp.status == 403:
            print(f"Comments are disabled for the video with ID: {video_id}")
            response = "Null"
        else:
            print(f"An error occurred while fetching comments for video ID: {video_id}")
            response = "Null"
    return response


def main():
    playlist_info={}
    comments_info={}
    user_input = st.text_input('Enter Channel Name')
    if st.button('submit'):
        channel_name = user_input
        progress_text = f"Fetching Data for channel {user_input}. Please wait."
        my_bar = st.progress(0, text=progress_text)
        my_bar.progress(10,"Connecting to Youtube Data API")
        youtube = create_youtube_service()
        my_bar.progress(15, "Connecting to MongoDB API")
        monodb_status, mg_collection = mongodb_connection()
        my_bar.progress(20, "Getting Channel ID")
        channel_id = yt_channel_id(youtube, channel_name)
        my_bar.progress(25, "Getting Channel Details")
        channel_details = yt_channel_details(youtube, channel_id)
        channel_info = {
            "channel_name": channel_details["items"][0]["snippet"]["title"],
            "channel_id": channel_details["items"][0]["id"],
            "description": channel_details["items"][0]["snippet"]["description"],
            "subscriber_count": channel_details["items"][0]["statistics"]["subscriberCount"],
            "view_count": channel_details["items"][0]["statistics"]["viewCount"],
            "video_count": channel_details["items"][0]["statistics"]["videoCount"],
            "Channel_Type": channel_details["items"][0].get("brandingSettings", {}).get("channel", {}).get("type",
                                                                                                           "Not Available"),
            "Channel_Status": channel_details["items"][0].get("brandingSettings", {}).get("channel", {}).get("status",
                                                                                                             "Not Available")
        }
        my_bar.progress(40, "Getting Playlist Details")
        playlist_details, playlist_ids = yt_playlists(youtube, channel_id)

        for playlist_count, id in enumerate(playlist_details):
            playlist_id = id["id"]
            playlist_name = id["snippet"]["title"]
            playlist_info.update({
                "playlist_id_" + str(playlist_count): playlist_id,
                "playlist_name_" + str(playlist_count): playlist_name
            })
            my_bar.progress(50, "Getting Video Details")
            video_ids = yt_video_details(youtube, playlist_id)
            for video_count, video_id in enumerate(video_ids):
                video_details = get_video_detail(youtube, video_id)
                comments_info.clear()
                if len(video_details["items"]) == 0:
                    continue
                #my_bar.progress(60, "Getting Comment Details")
                video_comments = yt_video_comments(youtube, video_id)
                if "items" in video_comments:
                    for comment_count, item in enumerate(video_comments["items"]):
                        comment = item["snippet"]["topLevelComment"]
                        comments_info.update({"comment_id_" + str(comment_count): {
                            "comment_id": item["id"],
                            "comment_text": comment["snippet"]["textDisplay"],
                            "comment_like_count": item["snippet"]["topLevelComment"]["snippet"]["likeCount"],
                            "Comment_Author": comment["snippet"]["authorDisplayName"],
                            "Comment_PublishedAt": comment["snippet"]["publishedAt"]
                        }})
                else:
                    comments_info = {}
                my_bar.progress(70, "Cleaning Data")
                playlist_info.update({
                    "video_id" + str(video_count) + "_" + str(playlist_count): {
                        "video_id": video_details["items"][0]["id"],
                        "title": video_details["items"][0]["snippet"]["title"],
                        "description": video_details["items"][0]["snippet"]["description"],
                        "Tags": video_details["items"][0]["snippet"]["tags"] if "tags" in video_details["items"][0][
                            "snippet"] else [],
                        "PublishedAt": video_details["items"][0]["snippet"]["publishedAt"],
                        "View_Count": int(video_details["items"][0]["statistics"]["viewCount"]),
                        "likes": video_details["items"][0]["statistics"].get("likeCount", 0),
                        "Favorite_Count": int(video_details["items"][0]["statistics"].get("favoriteCount", 0)),
                        "Comment_Count": int(video_details["items"][0]["statistics"].get("commentCount", 0)),
                        "Duration": video_details["items"][0]["contentDetails"]["duration"],
                        "Thumbnail": video_details["items"][0]["snippet"]["thumbnails"]["default"]["url"],
                        "Caption_Status": video_details["items"][0]["contentDetails"]["caption"],
                        "Comments": comments_info.copy()
                    }
                })

        channel_info.update(playlist_info)

        st.write(f"**Channel Name:** {channel_info['channel_name']}")
        st.write(f"**Channel ID:** {channel_info['channel_id']}")
        st.write(f"**Subscriber Count:** {channel_info['subscriber_count']}")
        st.write(f"**Total View Count:** {channel_info['view_count']}")
        st.write(f"**Total Video Count:** {channel_info['video_count']}")
        st.text_area("Raw Data", value=channel_info)
        my_bar.progress(80, "Inserting the Data to MongoDB")
        print("monodb_status", monodb_status)
        collection_insert_status = mg_collection.insert_one(channel_info)
        my_bar.progress(100, "Completed")
        print("collection_insert_status", collection_insert_status)
if __name__ == "__main__":
    main()
