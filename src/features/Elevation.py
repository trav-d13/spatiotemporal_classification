import os
import sys

import Config
from OpenMeteoApiTimer import enforce_request_interval, calculate_request_interval_batching, increase_interval

import numpy as np
import pandas as pd
import requests
import json

## SYSTEM LEVEL ##
file_name = 'elevation_final.csv'
"""string: file name of the output of the elevation extraction process"""
root_path = Config.root_dir()
"""string: The root file path of the project"""
data_path = '/data/processed/'
"""The data path to the directory of processed data."""
interim_data_file = 'interim_observations.csv'
"""string: File name where interim data is stored"""
interim_path = Config.root_dir() + "/data/interim/"
"""string: interim data directory path"""

## ELEVATION LEVEL ##
open_meteo_endpoint = 'https://api.open-meteo.com/v1/elevation?l'
"""string: Open-Meteo elevation API endpoint"""
position_elevation_dict = dict()
"""dict: Collection of coordinate to recorded observations acting as a cache"""
coordinate_accuracy = 4
"""int: Decimal places to round the coordinate values to"""
batch_size = 100
"""int: API parameter batch size"""
batch_limit = 100
"""int: The number of batches to be requesting during the course of execution"""
request_duration = 5
"""int: The number of minutes the batching of requests should extend over. Informs the GET request interval."""
current_batch_no = 0
"""int: The current batch number being requested"""
batch_start_index = 0
"""int: index tracker of the batch within the larger dataframe"""
current_batch = pd.DataFrame
"""DataFrame: The current batch """


def elevation_feature_extraction(df: pd.DataFrame):
    """Method performs the entirety of elevation extraction for all interim observations.

    This method includes the use of GET request limits (requests should not exceed 10000 a day, or more
    than 1 request per second. Open Meteo offers this API for non-commercial use, but it must be respected.

    Args:
        df (DataFrame): The dataframe containing the entirety of interim observations
    """
    global position_elevation_dict, current_batch_no
    calculate_request_interval_batching(batch_size, batch_limit, request_duration)
    df['elevation'] = None  # Create empty elevation column
    position_elevation_dict = collect_recorded_elevations()  # Read in already known elevations

    while batching(df):
        df = reduce_batch(df)  # Reduce batch first before getting coords
        observations_progress(df)  # Update current observations on the progress bar

        if not current_batch.empty:
            latitudes = current_batch['latitude'].tolist()  # Retrieve batch latitudes
            longitudes = current_batch['longitude'].tolist()  # Retrieve batch longitudes

            elevations = get_request(latitudes, longitudes)  # Retrieve coordinate elevations
            if elevations is None:
                continue

            current_batch['elevation'] = elevations  # Update current batch with elevation column

            position_elevation_dict = update_recorded_elevations(latitudes, longitudes,
                                                                 elevations)  # Update recorded positions
            df.update(current_batch)  # Merge batch back into dataframe

            current_batch_no = current_batch_no + 1  # Update batch number
            write_coordinate_elevation_dict(
                position_elevation_dict)  # Write the updated coordinate elevation dict to file
        sys.stdout.flush()

    df = final_processing(df)
    return df


def batching(df: pd.DataFrame):
    """This method separates df into a series of batches in order to perform batch API queries.

    Args:
        df (DataFrame): The whole dataframe containing interim observations. This df will be batched.

    Returns:
        A boolean indicating that the batching process is still ongoing. The method updates the global current_batch
        variable with a new batch in the process. If the batch limit or the end of the dataframe is reached, the method
        returns False.
    """
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
    """Method performs caching on the current batch. This caching reduces the number of queries send to the API

    Args:
        df (DataFrame): The current batch of observations to be queried for elevations.

    Returns:
        A modified df, where all cached elevations are appended the whole dataframe, and those observations are
        removed from the current batch.
    """
    global current_batch

    recorded_locations = current_batch.apply(
        lambda x: None if check_similar_location(x['latitude'], x['longitude']) is None
        else check_similar_location(x['latitude'], x['longitude']), axis=1).rename(
        'elevation')  # Determine already recorded elevations for similar locations

    recorded_filter = recorded_locations.isna()  # Create a mask, where non-recorded values are True
    if not recorded_locations.empty:  # Identifies similar elevations
        df.update(recorded_locations.to_frame())  # Merge found elevations into df
    current_batch = current_batch[recorded_filter]  # Update the current batch
    return df


def check_similar_location(latitude, longitude):
    """Method determines if the parameterized coordinate has a recorded elevation in the approximate area.

    This forms part of the caching process to determine approximately similar elevations.

    Args:
        latitude (float): A floating value representing latitude in the coordinate to be checked.
        longitude (float): A float value representing the longitude in the coordinate to be checked.

    Returns:
        Method returns a float representing the elevation (in meters) if the coordinate key is found.
        If no key is found, the exception is handled and None is returned.
    """
    rounded_lat = round(latitude, coordinate_accuracy)  # Round the latitude
    rounded_long = round(longitude, coordinate_accuracy)  # Round the longitude
    key = str(rounded_lat) + ", " + str(rounded_long)  # Create dictionary key string
    try:
        recorded_elevation = position_elevation_dict[key]  # Retrieve recorded elevation as similar coordinates
        return recorded_elevation
    except KeyError:
        return None


def update_recorded_elevations(latitudes, longitudes, elevations):
    """Method updates the coordinate-elevation dictionary based on the successful batch request.

    The coordinates are rounded to 4 decimal places in order to cache those in similar locations.
    Rounding to 4 decimal places produces an error of approximately 11.1m - 70m in coordinate accuracy.

    Args:
        latitudes (List): A list of numerical float latitudes
        longitudes (List): Alist of corresponding numerical float longitudes to latitudes.
        elevations (List): A list of corresponding elevations to the provided latitude, longitude pairs.
    """
    global position_elevation_dict
    np_latitudes = np.array(latitudes)  # Convert lats to np arrays for vector ops
    np_longitudes = np.array(longitudes)  # Convert longs to np arrays for vector ops

    np_latitudes_round = np.round(np_latitudes, coordinate_accuracy)  # Round the latitudes to correct accuracy
    np_longitudes_round = np.round(np_longitudes, coordinate_accuracy)  # Round the longitudes to the correct accuracy

    lats_round = np_latitudes_round.astype(str).tolist()  # Transform rounded lats to a list of strings
    longs_round = np_longitudes_round.astype(str).tolist()  # Transform rounded longs to a list of strings

    keys = [i + ", " + j for i, j in zip(lats_round, longs_round)]  # Create coordinate keys
    new_recordings = dict(zip(keys, elevations))  # Create new dictionary from coordinate keys and elevation values
    return {**position_elevation_dict, **new_recordings}  # merge new dictionary with current coordinate-elevation dict


def get_request(latitude, longitude):
    """Method performs the get request and data collection from the response from Open-Meteo Elevation API

    Method contains a request interval that is enforced by the OpenMeteoTimerAPI file. Please direct
    queries there for further information.

    Error responses (403-too many request error) is handled by increasing the timer interval, and re-attempting
    the batch request after the new interval.

    Each GET request is defined by a 5-second timeout parameter.

    Args:
        latitude (List): A float list containing a set of latitudes to be queries.
        longitude (List): A float corresponding list of longitudes to the provided latitudes.

    Returns:
        Elevation values for the queries coordinates
    """
    enforce_request_interval()  # Enforce the calculated interim request interval to respect the API
    params = {'latitude': latitude, 'longitude': longitude}  # Format the request parameters
    try:
        req = requests.get(url=open_meteo_endpoint, params=params, timeout=5)  # Perform GET request
        data = req.json()  # Retrieve data in JSON format
        return data['elevation']  # Return the retrieved data
    except Exception:
        print(" | Error occurred: 403")  # Assumed 403 exception
        increase_interval()  # Increase the request time interval


def final_processing(df: pd.DataFrame):
    """Method performs final processing of collected observations before being written to file.

    This file removes all elevations with a value of 0. This is indicative that the API request to Open-Meteo could not
    determine an elevation for the provided coordinates. These values should not be written to the final elevation store.

    Returns:
        The final processed dataframe containing the remaining recorded elevations to be written to processed storage.
        Additionally, a terminal print out indicating the amount of zero values removed.
    """
    processed_df = df[df.elevation != 0]
    removed_observations = df.shape[0] - processed_df.shape[0]
    print(" Observations with no elevation: ", removed_observations)
    return processed_df


def write_coordinate_elevation_dict(coordinate_elevations):
    """Method to write the current coordinate-elevation dictionary to the coordinate_elevation_store.txt file"""
    with open('coordinate_elevation_store.txt', 'w') as f:
        f.write(json.dumps(coordinate_elevations))


def collect_recorded_elevations() -> dict:
    """Method accesses the stored coordinate-api dictionary to serve as a cache to minimize API requests.

    Returns:
        If the coordinate_elevation_store.txt file exists, it returns the stored dictionary.
        Else an empty dictionary is returned.
    """
    if os.path.isfile('./coordinate_elevation_store.txt'):
        with open('coordinate_elevation_store.txt') as f:
            data = f.read()
            return json.loads(data)
    return {}


def write_recorded_elevations(df: pd.DataFrame):
    """Method writes all recorded elevations to elevation_final.csv inside the processed data folder.

    Note, only the elevations values with the corresponding index (id) are written to file.
    """
    final_elevations = df['elevation']
    final_elevations.to_csv(root_path + data_path + file_name, mode='w', index=True, header=True)


def import_interim_data():
    """Method to import interim_observations.csv as a dataframe

    Returns:
        A DataFrame containing all interim observations.
    """
    df = pd.read_csv(interim_path + interim_data_file)
    df.set_index('id', inplace=True, drop=True)
    return df


def observations_progress(df: pd.DataFrame):
    """Method to illustrate the observations that now contain elevations out of the entire dataset

    Args:
        df (DataFrame): The dataset containing all observations. This dataset is updated as elevations are generated.
    """
    cursor_up = '\x1b[1A'
    erase_line = '\x1b[2K'
    progress_bar_length = 100  # Specify progress bar length
    answered_observations = df[df.elevation >= 0]  # Determine the number of elevations recorded
    percentage_complete = answered_observations.shape[0] / df.shape[0]  # Calculate percentage complete
    filled = int(progress_bar_length * percentage_complete)  # Determine fill of percentage bar

    bar = '=' * filled + '-' * (progress_bar_length - filled)  # Modify bar with fill
    percentage_display = round(100 * percentage_complete, 3)  # Calculate percentage
    sys.stdout.write(cursor_up + erase_line + '\r[%s] %s%s ... current observations: %s / %s' % (bar,
                                                                                                 percentage_display,
                                                                                                 '%',
                                                                                                 answered_observations.shape[0],
                                                                                                 df.shape[0]))


if __name__ == '__main__':
    df = import_interim_data()
    df = elevation_feature_extraction(df)
    write_recorded_elevations(df)
