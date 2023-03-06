import pandas as pd
import requests
import json
from geopy import distance

open_meteo_endpoint = 'https://api.open-meteo.com/v1/elevation?l'
position_elevation_dict = {(-30.4901, 151.6393): 100}
coordinate_accuracy = 4
batch_size = 10
batch_start_index = 0
current_batch = pd.DataFrame


## Accuracy Loss ##
# Accuracy loss will range from 11.1m-70m. This should be included in method documentation


def elevation_feature_extraction(df: pd.DataFrame):
    while batching(df):
        # Reduce batch first before getting coords
        reduce_batch()
        latitudes = current_batch['latitude'].tolist()
        longitudes = current_batch['longitude'].tolist()

        # API request with remaining batch


def batching(df: pd.DataFrame):
    global batch_start_index, current_batch

    df_len = df.shape[0]
    batch_end_index = batch_start_index + batch_size  # Determine batch end index
    if batch_start_index > df_len:  # Batching stop condition
        return False

    if batch_end_index > df_len:  # Near end of dataset
        current_batch = df[['latitude', 'longitude']][batch_start_index:df_len]
    else:  # Full batch
        current_batch = df[['latitude', 'longitude']][batch_start_index:batch_end_index]

    batch_start_index = batch_end_index  # Update batch start index
    return True


def reduce_batch():
    recorded_filter = current_batch.apply(
        lambda x: False if check_similar_location(x['latitude'], x['longitude']) is None
        else check_similar_location(x['latitude'], x['longitude']), axis=1)
    #TODO Remove rows with recorded elevation at coordinates
    #TODO Update the dataframe
    #TODO Write dictionary to file
    #TODO Place API limiter on
    print(recorded_filter)


def check_similar_location(latitude, longitude):
    rounded_lat = round(latitude, coordinate_accuracy)
    rounded_long = round(longitude, coordinate_accuracy)
    key = (rounded_lat, rounded_long)
    print(key)
    try:
        recorded_elevation = position_elevation_dict[key]
        return recorded_elevation
    except KeyError:
        return None


def get_request(latitude, longitude):
    params = {'latitude': latitude, 'longitude': longitude}
    req = requests.get(url=open_meteo_endpoint, params=params)
    data = req.json()
    print(data)
