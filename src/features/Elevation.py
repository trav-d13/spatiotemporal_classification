import pandas as pd
import requests
import json


open_elevation_endpoint = 'https://api.open-elevation.com/api/v1/lookup'


def elevation_feature_extraction(df: pd.DataFrame):
    request_data = format_post_request(df)
    request = send_post(request_data)
    print(request)


def send_post(formatted_post_data: dict):
    request = requests.post(url=open_elevation_endpoint,
                            headers={'Content-Type': 'application.json',
                                     'Accept': 'application/json'},
                            data=formatted_post_data)
    return request


def format_post_request(df: pd.DataFrame):
    formatted_coordinates = format_coordinates(df)
    post_request = {'locations': formatted_coordinates}
    json_data = json.dumps(post_request).encode('utf-8')
    return post_request


def format_coordinates(df: pd.DataFrame):
    coordinates_df = df[['latitude', 'longitude']]  # Generate DataFrame of latitude, and longitude coordinates
    locations_series = coordinates_df.apply(format_coordinate_dict, axis=1)
    locations_list = locations_series.to_list()
    return locations_list


def format_coordinate_dict(x):
    location_dict = {"latitude": x['latitude'],
                    "longitude": x['longitude']}
    return location_dict
