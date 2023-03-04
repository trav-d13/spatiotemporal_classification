## FEATURE EXTRACTION PIPELINE ##

import pandas as pd
from Config import root_dir

interim_data_file = 'interim_sample.csv'
interim_path = root_dir() + "/data/interim/"


def feature_extraction():
    df = pd.read_csv(interim_path + interim_data_file)
    print(df.head())


if __name__ == '__main__':
    feature_extraction()