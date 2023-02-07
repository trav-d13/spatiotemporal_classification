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


if __name__ == "__main__":
    generate_dataframe(dataset)

