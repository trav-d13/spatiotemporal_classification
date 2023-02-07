import pandas as pd
from Config import Definitions

dataset = "observations_1.csv"
observation_path = "/data/raw/"


def generate_dataframe(observation_file) -> pd.DataFrame:
    file_path = Definitions.root_dir() + \
               observation_path + \
               observation_file
    df = pd.read_csv(file_path, sep=",")
    return df


def sighting_duplication_removal(df) -> int:
    return df.drop_duplicates(subset=['id'], keep='first')


if __name__ == "__main__":
    sighting_df = generate_dataframe(dataset)
    print(sighting_duplication_removal(sighting_df))

