import pandas as pd

from Config import root_dir

from datetime import datetime


class Pipeline:
    """ Pipeline to clean raw data into interim data source.

    Args:
        df DataFrame: Dataframe tp hold aggregated raw data
        datasets list: A list of individual observation csv files to be aggregated as raw data
        resource_path str: Path to raw data resources, from project root directory
        test_df DataFrame: A direct dataframe insert for pipeline testing purposes
    """

    def __init__(self, datasets=['observations_sample.csv'], test_df=None):
        if test_df is None:
            self.df = pd.DataFrame()
            self.datasets = datasets
            self.resource_path = root_dir() + "/data/raw/"
        else:
            self.df = test_df.copy(deep=True)

    def aggregate_observations(self):
        """Method aggregates all observations from separate files, placing them within a df for manipulation"""
        for dataset in self.datasets:
            df_temp = pd.read_csv(self.resource_path + dataset)
            self.df = pd.concat([self.df, df_temp])

    def enforce_unique_ids(self):
        """Removal of any duplicate observations utilizing their observation id

        In place duplicate removal such that changes are effected directly within df
        """
        self.df.drop_duplicates(subset=['id'], keep='first', inplace=True)

    def format_observation_dates(self):
        self.df['observed_on'] = pd.to_datetime(self.df['observed_on'],
                                                format='%Y-%m-%d',
                                                yearfirst=True,
                                                errors='coerce',
                                                exact=True).astype(str)
        self.df.query('observed_on != "NaT"', inplace=True)
        self.df.reset_index(drop=True, inplace=True)


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.aggregate_observations()
    pipeline.enforce_unique_ids()
