import pandas as pd
from Config import root_dir


class Pipeline:
    """ Pipeline to clean raw data into interim data source.

    Attributes:
        observation_path (str): The relative path (from project root) to the raw data observations

    Args:
        dataset (str): file containing observation set
    """

    def __init__(self, datasets=['observations_sample.csv'], test_df=None):
        self.raw_df = pd.DataFrame()
        self.datasets = datasets
        self.resource_path = root_dir() + "/data/raw/"

    def aggregate_observations(self):
        for dataset in self.datasets:
            df_temp = pd.read_csv(self.resource_path + dataset)
            self.raw_df = pd.concat([self.raw_df, df_temp])
        print(self.raw_df.head())


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.aggregate_observations()

