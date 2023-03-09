import os
from time import sleep

import numpy as np
import pandas as pd
import requests
import json

open_meteo_endpoint = 'https://api.open-meteo.com/v1/elevation?l'
position_elevation_dict = dict()
coordinate_accuracy = 4
batch_size = 100
batch_limit = 10
current_batch_no = 0
batch_start_index = 0
current_batch = pd.DataFrame


## Accuracy Loss ##
# Accuracy loss will range from 11.1m-70m. This should be included in method documentation


def elevation_feature_extraction(df: pd.DataFrame):
    global position_elevation_dict, current_batch_no
    df['elevation'] = None  # Create empty elevation column
    position_elevation_dict = collect_recorded_elevations()  # Read in already known elevations
    while batching(df):
        df = reduce_batch(df)  # Reduce batch first before getting coords

        if not current_batch.empty:
            latitudes = current_batch['latitude'].tolist()  # Retrieve batch latitudes
            longitudes = current_batch['longitude'].tolist()  # Retrieve batch longitudes

            elevations = get_request(latitudes, longitudes)  # Retrieve coordinate elevations
            current_batch['elevation'] = elevations  # Update current batch with elevation column

            position_elevation_dict = update_recorded_elevations(latitudes, longitudes, elevations)  # Update recorded positions
            df.update(current_batch)  # Merge batch back into dataframe

            current_batch_no = current_batch_no + 1  # Update batch number

    write_coordinate_elevation_dict(position_elevation_dict)  # Write the updated coordinate elevation dict to file
    return df


def batching(df: pd.DataFrame):
    global batch_start_index, current_batch, current_batch_no

    df_len = df.shape[0]
    batch_end_index = batch_start_index + batch_size  # Determine batch end index
    if current_batch_no == batch_limit or batch_start_index > df_len:  # Batching stop conditions
        return False

    if batch_end_index > df_len:  # Near end of dataset
        current_batch = df[['latitude', 'longitude']][batch_start_index:df_len]
    else:  # Full batch
        current_batch = df[['latitude', 'longitude']][batch_start_index:batch_end_index]

    batch_start_index = batch_end_index  # Update batch start index

    return True


def reduce_batch(df: pd.DataFrame):
    global current_batch

    recorded_locations = current_batch.apply(
        lambda x: None if check_similar_location(x['latitude'], x['longitude']) is None
        else check_similar_location(x['latitude'], x['longitude']), axis=1).rename('elevation')     # Determine already recorded elevations for similar locations

    recorded_filter = recorded_locations.isna()  # Create a mask, where non-recorded values are True
    if not recorded_locations.empty:  # Identifies similar elevations
        df.update(recorded_locations.to_frame())  # Merge found elevations into df
    current_batch = current_batch[recorded_filter]  # Update the current batch
    return df


def check_similar_location(latitude, longitude):
    rounded_lat = round(latitude, coordinate_accuracy)  # Round the latitude
    rounded_long = round(longitude, coordinate_accuracy)  # Round the longitude
    key = str(rounded_lat) + ", " + str(rounded_long)  # Create dictionary key string
    try:
        recorded_elevation = position_elevation_dict[key]  # Retrieve recorded elevation as similar coordinates
        return recorded_elevation
    except KeyError:
        return None


def update_recorded_elevations(latitudes, longitudes, elevations):
    global position_elevation_dict
    np_latitudes = np.array(latitudes)  # Convert lats to np arrays for vector ops
    np_longitudes = np.array(longitudes)   # Convert longs to np arrays for vector ops

    np_latitudes_round = np.round(np_latitudes, coordinate_accuracy)  # Round the latitudes to correct accuracy
    np_longitudes_round = np.round(np_longitudes, coordinate_accuracy)  # Round the longitudes to the correct accuracy

    lats_round = np_latitudes_round.astype(str).tolist()  # Transform rounded lats to a list of strings
    longs_round = np_longitudes_round.astype(str).tolist()  # Transform rounded longs to a list of strings

    keys = [i + ", " + j for i, j in zip(lats_round, longs_round)]  # Create coordinate keys
    new_recordings = dict(zip(keys, elevations))  # Create new dictionary from coordinate keys and elevation values
    return {**position_elevation_dict, **new_recordings}  # merge new dictionary with current coordinate-elevation dict


def get_request(latitude, longitude):
    params = {'latitude': latitude, 'longitude': longitude}
    req = requests.get(url=open_meteo_endpoint, params=params)
    sleep(1)
    data = req.json()
    return data['elevation']


def write_coordinate_elevation_dict(coordinate_elevations):
    with open('coordinate_elevation_store.txt', 'w') as f:
        f.write(json.dumps(coordinate_elevations))


def collect_recorded_elevations() -> dict:
    if os.path.isfile('./coordinate_elevation_store.txt'):
        with open('coordinate_elevation_store.txt') as f:
            data = f.read()
            return json.loads(data)
    return {}
