import pandas as pd


def format_request(df: pd.DataFrame):
    locations = df[['latitude', 'longitude']]
    print(locations)