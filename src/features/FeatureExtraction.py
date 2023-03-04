## FEATURE EXTRACTION PIPELINE ##

import pandas as pd
from Config import root_dir
from Elevation import format_request

interim_data_file = 'interim_sample.csv'
interim_path = root_dir() + "/data/interim/"


def feature_extraction():
    df = import_interim_data()  # Import interim data

    # Extract Elevation
    format_request(df)


def import_interim_data():
    df = pd.read_csv(interim_path + interim_data_file)
    df.set_index('id', inplace=True, drop=True)
    return df


if __name__ == '__main__':
    feature_extraction()

