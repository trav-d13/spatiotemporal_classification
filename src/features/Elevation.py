import pandas as pd
import requests
import json


open_meteo_endpoint = 'https://api.open-meteo.com/v1/elevation?l'


def elevation_feature_extraction(df: pd.DataFrame):
    lats = [df['latitude'][128984633], df['latitude'][129048796]]
    longs = [df['longitude'][128984633], df['longitude'][129048796]]
    get_request(lats, longs)


def get_request(latitude, longitude):
    params = {'latitude': latitude, 'longitude': longitude}
    req = requests.get(url=open_meteo_endpoint, params=params)
    data = req.json()
    print(data)
