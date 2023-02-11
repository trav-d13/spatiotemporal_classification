import pandas as pd

from Config import root_dir


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
            self.write_path = root_dir() + "/data/interim/"
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
        """ Method ensures that raw data dates follow format yyyy-mm-dd. If the dates deviate they are removed from the dataframe.

        In place changes are effected within the classes dataframe df.
        Any date errors produced by incorrect date formats are transformed into NaT values, and the row removed from df
        Removal of the rows requires an index reset in order to facilitate testing operations on the cleaning pipeline.
        """
        self.df['observed_on'] = pd.to_datetime(self.df['observed_on'],
                                                format='%Y-%m-%d',
                                                yearfirst=True,
                                                errors='coerce',
                                                exact=True).astype(str)
        self.df.query('observed_on != "NaT"', inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    def write_interim_data(self):
        """ Method writes current state of df into interim data folder in csv format"""
        file_name = "interim_observations.csv"
        self.df.to_csv(self.write_path + file_name, index=False)


if __name__ == "__main__":
    # Create Pipeline object
    pipeline = Pipeline()

    # Aggregate all observation files
    pipeline.aggregate_observations()

    # Ensure that no sighting duplicates are within the aggregate set
    pipeline.enforce_unique_ids()

    # Ensure that sighting dates follow the same format.
    pipeline.format_observation_dates()

    pipeline.write_interim_data()

    # Display df head
    print(pipeline.df.head())
