import pandas as pd
from Config import Definitions


class Pipeline:
    """:class: Pipeline to clean raw data into interim data source"""

    def __init__(self, datasets):
        self.dataset = "observations_1.csv"
        self.observation_path = "/data/raw/"

    def generate_dataframe(self, observation_file) -> pd.DataFrame:
        """Generate a dataframe from a parameterized observation .csv file
        :param observation_file: A .csv file containing the raw observations
        :return: Pandas dataframe: A dataframe representation of the observations
        """
        file_path = Definitions.root_dir() + \
                    self.observation_path + \
                    observation_file
        df = pd.read_csv(file_path, sep=",")
        return df

    def sighting_duplication_removal(self, df) -> pd.DataFrame:
        """Remove any possible sighting duplications from the dataframe
        :param df: Pandas dataframe containing sightings
        :return:  Pandas dataframe: Modified dataframe with duplications removed
        """
        return df.drop_duplicates(subset=['id'], keep='first')
