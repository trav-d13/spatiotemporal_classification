import pandas as pd
from Config import root_dir


class Pipeline:
    """ Pipeline to clean raw data into interim data source.

    Attributes:
        observation_path (str): The relative path (from project root) to the raw data observations

    Args:
        dataset (str): file containing observation set
    """

    observation_path = "/data/raw/"

    def __init__(self, datasets):
        self.dataset = "observations_1.csv"

    def generate_dataframe(self, observation_file) -> pd.DataFrame:
        """Generate a dataframe from a parameterized observation csv file.

        Args:
            observation_file (str): A csv file containing the raw observations

        Returns:
            DataFrame: A dataframe representation of the observations
        """
        file_path = root_dir() + \
                    self.observation_path + \
                    observation_file
        df = pd.read_csv(file_path, sep=",")
        return df

    def sighting_duplication_removal(self, df) -> pd.DataFrame:
        """Remove any possible sighting duplications from the dataframe.

        Args:
            df (DataFrame): Dataframe format of observations

        Returns:
            DataFrame: Modified dataframe with duplications removed
        """
        return df.drop_duplicates(subset=['id'], keep='first')
