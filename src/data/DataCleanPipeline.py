import pandas as pd
import os
import sys
from timezonefinder import TimezoneFinder
import pytz
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from functools import partial
from Config import root_dir
from datetime import datetime


class Pipeline:
    """ Pipeline to clean raw data into interim data source.

    Args:
        df_whole (DataFrame): Contains all aggregated observations
        df (DataFrame): Dataframe containing the current batch of observations
        datasets (list): A list of individual observation csv files to be aggregated as raw data
        resource_path (str): Path to raw data resources, from project root directory
        write_path (str): Path to interim data resources, from project root directory
        interim_exists (bool): A flag representing if an existing interim_data.csv file exists in the project.
        row_sum (int): Contains the sum of aggregate observations. Value only initialized after dataset aggregation.
        start_time (DateTime): Records the start time of pipeline processing
        TEST (bool): A flag indicating values should be initialized for testing purposes.
        test_df (DataFrame): A direct dataframe insert for pipeline testing purposes
    """

    interim_file = "interim_observations.csv"
    """string: Specification of the file to write data to after cleaning process."""
    bad_file = 'bad_quality.csv'
    batch_size = 1000
    """int: Size of individual batches that aggregate observations are broken down into."""

    def __init__(self, datasets=['observations_sample.csv'], test_df=None):
        if test_df is None:
            self.df_whole = pd.DataFrame()
            self.df = pd.DataFrame()
            self.datasets = datasets
            self.resource_path = root_dir() + "/data/raw/"
            self.write_path = root_dir() + "/data/interim/"
            self.interim_exists = os.path.isfile(self.write_path + self.interim_file)
            self.bad_data_exists = os.path.isfile(self.write_path + self.bad_file)
            self.row_sum = 0
            self.TEST = False
        else:
            self.df_whole = test_df.copy(deep=True)
            self.TEST = True
            self.row_sum = len(self.df_whole.index)

        self.start_time = datetime.now()

    def activate_flow(self):
        """ Method details and executes the flow of the cleaning pipeline"""

        self.aggregate_observations()  # Aggregate all observation files

        self.enforce_unique_ids()  # No duplicate observations

        self.continuation()  # Continuation from interrupt/ start from scratch

        self.remove_na_working_columns()  # Remove any NaN types from columns undergoing computation

        while self.batching():  # Batching loop
            self.bad_data_separation()  # Detect bad quality observations from batch

            self.format_observation_dates()  # Format sighting dates

            self.generate_local_times()  # Generate local observation times

            self.remove_peripheral_columns()  # Remove peripheral columns

            self.write_interim_data()  # Write to interim data

    def aggregate_observations(self):
        """Method aggregates all observations from separate files, placing them within a df for manipulation

        Method will check if dataframe is empty. This is to accommodate test cases which preloads the dataframe into
         the dataframe
        """
        if not self.TEST:
            for dataset in self.datasets:
                df_temp = pd.read_csv(self.resource_path + dataset)
                self.df_whole = pd.concat([self.df_whole, df_temp])  # Merge temp df into container df

    def enforce_unique_ids(self):
        """Removal of any duplicate observations utilizing their observation id

        In place duplicate removal such that changes are effected directly within df
        """
        self.df_whole.drop_duplicates(subset=['id'], keep='first', inplace=True)

    def continuation(self, test_interim_df=None, test_bad_df=None):
        """ Method determines the status of the Data Cleaning Pipeline, enabling continuation without redundancies if
        the cleaning process is interrupted.

        This method enables testing through the test_interim_df dataframe that can be passed to it.
        However, within the pipeline this is automated, and no parameter is required.
        Method accesses the interim_data.csv file, and identifies already processes observations, removing them from the
        current cleaning proces.

        Args:
            test_interim_df (DataFrame): This dataframe is None during Pipeline cleaning process, however it allows for the creation
            of an interim dataframe for testing purposes (Not running the entire pipeline.
        """
        self.df_whole.set_index('id', inplace=True)

        interim_df = pd.DataFrame()
        bad_quality_df = pd.DataFrame()

        if self.TEST and test_interim_df is not None:  # Conditions for test cases
            interim_df = pd.concat([interim_df, test_interim_df])
            bad_quality_df = pd.concat([bad_quality_df, test_bad_df])

        if not self.TEST and self.interim_exists:  # Non-test conditions when interim data file exists
            interim_df = pd.read_csv(self.write_path + self.interim_file)
        if not self.TEST and self.bad_data_exists:  # Non-test conditions when bad_quality data file exists
            bad_quality_df = pd.read_csv(self.write_path + self.bad_file)

        if not interim_df.empty:  # Removal of duplicate rows already written to interim data
            interim_df.set_index('id', inplace=True)
            self.df_whole = self.df_whole.loc[self.df_whole.index.difference(interim_df.index), ]

        if not bad_quality_df.empty:  # Removal of duplicate rows already written to bad_quality data
            bad_quality_df.set_index('id', inplace=True)
            self.df_whole = self.df_whole.loc[self.df_whole.index.difference(bad_quality_df.index), ]

        self.row_sum = len(self.df_whole.index)

    def remove_na_working_columns(self):
        """ This method removes all rows with NaN values, specifically located within columns used for computation that require
        values.

         The 'working columns' include date, time, time zone, and coordinates.
         If the removal creates an empty dataframe, the method exists execution, displaying an exit message.
        """
        self.df_whole.dropna(subset=['observed_on', 'latitude', 'longitude', 'time_observed_at', 'time_zone'],
                             inplace=True)
        if self.df_whole.empty:
            print("*********** No further correctly format to process ***********")
            sys.exit()

    def batching(self) -> bool:
        """ This method creates observation batches from the aggregate observations in order to iteratively process and clean
        data.

        This method processes each batch, iteratively writing to interim_data.csv.
        The DataFrame df_whole contains aggregate observations, while df contains the current batch.
        Each batch once generated is removed from df_whole until this dataframe is empty, concluding the batching process.

        Returns:
            Method returns a boolean value. True if there are still observations to be batched and processes. False if df_whole is empty
            indicating the batching process has concluded.
        """
        rows_remaining = len(self.df_whole.index)
        if not self.TEST: self.percentage(rows_remaining)

        if self.df_whole.empty:
            return False

        if rows_remaining > self.batch_size:
            self.df = self.df_whole.iloc[0:self.batch_size]
            self.df_whole = self.df_whole.iloc[self.batch_size:]
            return True
        else:
            self.df = self.df_whole
            self.df_whole = pd.DataFrame()
            return True

    def percentage(self, rows_remaining):
        """ Method generates and updates a status bar based on the progress of batching.

        Both percentage complete and running time metrics are displayed
        Method inspiration: Inspiration: https://www.geeksforgeeks.org/progress-bars-in-python/

        Args:
            rows_remaining (int): The number of rows remaining to be processed in the df_whole DataFrame.
        """
        progress_bar_length = 100
        percentage_complete = (self.row_sum - rows_remaining) / self.row_sum
        filled = int(progress_bar_length * percentage_complete)
        running_time = datetime.now() - self.start_time

        bar = '=' * filled + '-' * (progress_bar_length - filled)
        percentage_display = round(100 * percentage_complete, 1)
        sys.stdout.write('\r[%s] %s%s ... running: %s' % (bar, percentage_display, '%', running_time))
        sys.stdout.flush()

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

    def coordinate_to_country_rate_limited(self):
        """ Method takes data coordinates, and identifies the country of origin, creating a Country column within Interim data

        The Nomanitim geocoding API is utilized.
        Due to rate limiting of 1 request per second, a rate limiter has been introduced in order to respect the limits.
        """
        # Set up the geolocation library
        geolocator = Nominatim(user_agent="Spatio_Tempt_Class")
        geocode = RateLimiter(geolocator.reverse, min_delay_seconds=2)

        # Combine lat and long into coordinates
        latitudes = pd.Series(self.df.latitude.values.astype(str))
        longitudes = pd.Series(self.df.longitude.values.astype(str))
        coordinates = latitudes + ', ' + longitudes

        # Retrieve countries from coordinates (rate limiting requests)
        locations = coordinates.apply(partial(geocode, language='en', exactly_one=True))
        self.df['country'] = locations.apply(lambda x: x.raw['address']['country']).values

    def generate_local_times(self):
        """ Method converts UTC time to correct local time utilizing the specified sighting timezone.

        The new column generated is labelled "local_time_observed_at" and can be found in interim data folder.

        The date conversion utilizes both date and time in the UTC format.
        This means, any day change (midnight -> next day) are accounted for.
        The observed_on column is correct.
        """

        # Standardize time zone formats
        self.standardize_timezones()

        # Generate local times by converting UTC to specified time zones
        self.df['local_time_observed_at'] = self.df.apply(
            lambda x: pd.to_datetime(x['time_observed_at'], utc=True).astimezone(pytz.timezone(x['time_zone'])),
            axis=1).astype(str)

    def standardize_timezones(self):
        """ Method generated timezones in a format accepted by the pytz library for use in the local time zone conversion

        This method utilizes the observation coordinates to return the time zone of the sighting.
        This timezone overwrites the "time_zone" column
        """
        finder = TimezoneFinder()

        self.df['time_zone'] = self.df.apply(
            lambda x: finder.timezone_at(lat=x['latitude'], lng=x['longitude']), axis=1)

    def bad_data_separation(self):
        """Method performs the sub-process of bad data separation, formatting, and writing to file"""
        bad_df = self.identify_bad_observations()
        bad_df = self.format_bad_data(bad_df)
        self.write_bad_data(bad_df)

    def identify_bad_observations(self):
        """Method identifies probable bad quality images based on keyword extraction from the descriptions

        Keyword pattern identification is accomplished through the use of regex pattern matching
        """
        description_indicators = ['dead', 'road kill', 'road', 'scat', 'poo', 'killed', 'spoor', 'road-kill', 'remains',
                                  'body', 'deceased', 'prey', 'fatality', 'tracks', 'trapped', 'bad', 'roadkilled', 'poop',
                                  'crushed', 'kill', 'squashed', 'terrible', 'caught', 'pool', 'blurry', 'destroyed',
                                  'sidewalk',
                                  'grounded']
        regex_pattern = '|'.join([f'{key_word}' for key_word in description_indicators])
        filter = self.df.description.str.contains(regex_pattern, case=False, regex=True)  # Filter descriptions to identify keywords
        filter.fillna(False, inplace=True)
        bad_df = self.df[filter]
        self.df = self.df[~filter]
        bad_df['image_quality'] = 'bad'
        return bad_df

    def remove_peripheral_columns(self):
        """ Method removes all peripheral columns before writing dataframe to interim_data.csv"""
        self.df = self.df[['observed_on', 'local_time_observed_at', 'latitude', 'longitude',
                           'positional_accuracy', 'public_positional_accuracy', 'image_url', 'license', 'geoprivacy',
                           'taxon_geoprivacy', 'scientific_name', 'common_name', 'taxon_id']]

    def format_bad_data(self, bad_df):
        """Method creates a sub-dataframe of only the image_url and the image quality (Index is observation ID)

        This subset creates the format that the bad quality images will be written to bad_quality.csv

        Args:
            bad_df (DataFrame): DataFrame containing all bad observations filtered from df
        """
        bad_df = bad_df[['image_url', 'image_quality']]
        return bad_df

    def write_bad_data(self, bad_df):
        """Method performs similar operation to the write_interim_data() method, in this case specifically writing bad data

        This method should be refactored in conjunction with write_interim_data in order to minimize code repetition

        Args:
            bad_df (DataFrame): DataFrame containing the sub-dataframe of only id, image_url, and image_quality columns
        """
        if not self.TEST:
            self.bad_data_exists = os.path.isfile(self.write_path + self.bad_file)
            if self.bad_data_exists:
                bad_df.to_csv(self.write_path + self.bad_file, mode='a', index=True, header=False)
            else:
                bad_df.to_csv(self.write_path + self.bad_file, mode='w', index=True, header=True)

    def write_interim_data(self):
        """ Method writes current state of df into interim data folder in csv format"""
        if not self.TEST:
            self.interim_exists = os.path.isfile(self.write_path + self.interim_file)
            if self.interim_exists:
                self.df.to_csv(self.write_path + self.interim_file, mode='a', index=True, header=False)
            else:
                self.df.to_csv(self.write_path + self.interim_file, mode='w', index=True, header=True)


if __name__ == "__main__":
    # Create Pipeline object
    pipeline = Pipeline()

    # Activate pipeline flow
    pipeline.activate_flow()
