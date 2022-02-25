import pandas as pd
from googleapiclient.discovery import build
import requests
import json
import os
import glob
import time
from tqdm import tqdm
from datetime import datetime
import isodate
import math

### CREDENTIALS
DEVELOPER_KEY = "API KEY"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

### DATASET TO PROCESS

file = "list_of_jobs_sample.csv"

token = "" # Dummie Variable
n = 1 # Loop counter set to 1. Needed for iterations in get_channel_videos.

def get_channel_videos(channel_id, n, token, output_folder):
    if n == 1:
        # First token request
        response = requests.get(f"https://www.googleapis.com/youtube/v3/search?key={DEVELOPER_KEY}&channelId={channel_id}&part=snippet,id&order=date&maxResults=50")
    else:
        # This query is for next token
        response = requests.get(f"https://www.googleapis.com/youtube/v3/search?key={DEVELOPER_KEY}&channelId={channel_id}&part=snippet,id&order=date&maxResults=50&pageToken={token}")

    json_data = json.loads(response.text)

    try:
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        with open((output_folder+ f"chan_{channel_id}_loop_{n}.json"), 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)
    except IndexError:
        print("ERROR")
        pass
    try:
        if json_data["nextPageToken"]:
            token = json_data["nextPageToken"]
            n = n + 1
            get_channel_videos(channel_id, n, token, output_folder)
    except KeyError:
        print("Last Page")

def get_videos_data(channel_id, reference_date, output_folder):
    files = glob.glob(f"channel_data_responses/{channel_id}/*.json")
    jobs_to_do = []
    for file in files:
        with open(file, encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)
            videos = data["items"]

            for video in videos:
                try:
                    #### Comparing video publishedAt date with reference date (need to convert to datetype object
                    video_date = video["snippet"]["publishedAt"]
                    video_date = pd.to_datetime(video_date, format="%Y-%m-%dT%H:%M:%SZ")
                    reference_date = pd.to_datetime(reference_date, format="%Y-%m-%d %H:%M:%S")

                    video_id = video["id"]["videoId"]

                    if video_date > reference_date:
                        jobs_to_do.append(video_id)
                    else:
                        pass
                except KeyError:
                    pass

    jobs = len(jobs_to_do)+1
    print(f"total new videos for {channel_id} = {jobs}")
    for video in tqdm(jobs_to_do):
        work = jobs_to_do.index(video)+1

        vid_statistics = youtube.videos().list(
            id=video,
            part="statistics, contentDetails, snippet",
        ).execute()

        try:
            with open((output_folder + f"vid_{video}__channel_{channel_id}.json"), 'w', encoding='utf-8') as f:
                json.dump(vid_statistics, f, ensure_ascii=False, indent=4)
            time.sleep(0.5) # avoid api limit
        except IndexError:
            pass

def parser(channel_id, request_time):

    video_files = glob.glob(f"channel_data_responses/{channel_id}/vid_*.json")
    channel_files = glob.glob(f"channel_data_responses/{channel_id}/chan_*.json")

    video_collection_list = []

    for file in channel_files:
        with open(file, encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)
            videos = data["items"]
            for video in videos:
                try:
                    videoId = video["id"]["videoId"]
                    publishedAt = video["snippet"]["publishedAt"]
                    channelId = video["snippet"]["channelId"]
                    channelTitle = video["snippet"]["channelTitle"]
                    video_title = video["snippet"]["title"]
                    video_description = video["snippet"]["description"]
                except KeyError:
                    pass

                video_data = glob.glob(f"channel_data_responses/{channel_id}/vid_{videoId}*.json")
                for item in video_data:
                    with open(item, encoding="utf-8") as jsonfile:
                        info = json.load(jsonfile)
                        data = info["items"][0]
                        # YOUTUBE API ISO 8601 PARSER TO SECONDS
                        duration = data["contentDetails"]["duration"]
                        dur = isodate.parse_duration(duration)
                        print(dur.total_seconds())
                        duration = dur.total_seconds()
                        duration = int(duration)


                        dimension = data["contentDetails"]["dimension"]
                        definition = data["contentDetails"]["definition"]
                        caption = data["contentDetails"]["caption"]
                        licensedContent = data["contentDetails"]["licensedContent"]
                        viewCount = data["statistics"]["viewCount"]
                        likeCount = data["statistics"]["likeCount"]
                        favoriteCount = data["statistics"]["favoriteCount"]
                        commentCount = data["statistics"]["commentCount"]
                        try:
                            tags = data["snippet"]["tags"]
                            tags = [tags]
                        except KeyError:
                            tags = "false"
                        categoryId = data["snippet"]["categoryId"]
                        defaultLanguage = data["snippet"]["defaultLanguage"]
                        defaultAudioLanguage = data["snippet"]["defaultAudioLanguage"]

                    df = pd.DataFrame({
                        "id": "none",
                        "hash": videoId,
                        "channelId": channelId,
                        "channelTitle": channelTitle,
                        "publishedAt":publishedAt,
                        "title":video_title,
                        "description":video_description,
                        "tags": tags,
                        "categoryId": categoryId,
                        "defaultLanguage": defaultLanguage,
                        "defaultAudioLanguage": defaultAudioLanguage,
                        "duration": duration,
                        "dimension": dimension,
                        "definition": definition,
                        "caption": caption,
                        "licensedContent": licensedContent,
                        "allowedIn": "none",
                        "blockedIn": "none",
                        "viewCount": viewCount,
                        "likeCount": likeCount,
                        "dislikeCount": "none",
                        "favoriteCount": favoriteCount,
                        "commentCount": commentCount,
                        "requesttime": request_time,
                    }, index=[0])
                    video_collection_list.append(df)
    final_df = pd.concat(video_collection_list)

    # Change this line to dump on MySQL DB.
    # Program routine within YouTube's API rate limits
    final_df.to_csv(f"channel_data_responses/{channel_id}/new_videos_for_{channel_id}.csv", index=False, sep=",",quotechar='"', line_terminator="\n")

    # AFEGIR MYSQL AQUI



jobs = pd.read_csv(f"{file}")
for index, row in jobs.iterrows():

    # Data for the requests
    request_time = datetime.now()
    channel_id = row["channel_id"] # the channel to check
    reference_date = row["last_date"] # date of the last video in our dataset
    output_folder = f"channel_data_responses/{channel_id}/" # generate an outpufloder

    try:
        if math.isnan(reference_date) == True:
            print("channel break")
            jobs.loc[jobs["channel_id"] == channel_id, "status"] = "done"
            print(jobs)
            jobs.to_csv(f"{file}", index=False)
            pass
    except TypeError:
        if row["status"] == "done":
            pass
        else:
            # EXECUTION SEQUENCE
            get_channel_videos(channel_id, n, token, output_folder) # Get Channel Videos (complete collection)
            get_videos_data(channel_id, reference_date, output_folder) # Detect new videos and collect Json Data
            parser(channel_id, request_time) # Parse Json files and create a .csv file for each channel with new videos

            # If using a MYSQL Dump, remember using a connection.close()

            # Save channel ID status
            jobs.loc[jobs["channel_id"] == channel_id, "status"] = "done"
            jobs.to_csv(f"{file}", index=False)

