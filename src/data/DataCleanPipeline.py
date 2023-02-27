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
        df DataFrame: Dataframe tp hold aggregated raw data
        datasets list: A list of individual observation csv files to be aggregated as raw data
        resource_path str: Path to raw data resources, from project root directory
        test_df DataFrame: A direct dataframe insert for pipeline testing purposes
    """

    interim_file = "interim_observations.csv"
    batch_size = 1000

    def __init__(self, datasets=['observations_sample.csv'], test_df=None):
        if test_df is None:
            self.df_whole = pd.DataFrame()
            self.df = pd.DataFrame()
            self.datasets = datasets
            self.resource_path = root_dir() + "/data/raw/"
            self.write_path = root_dir() + "/data/interim/"
            self.interim_exists = os.path.isfile(self.write_path + self.interim_file)
            self.row_sum = 0
            self.TEST = False
        else:
            self.df_whole = test_df.copy(deep=True)
            self.TEST = True
            self.row_sum = len(self.df_whole.index)

        self.start_time = datetime.now()

    def activate_flow(self):
        # Aggregate all observation files
        self.aggregate_observations()

        # Ensure that no sighting duplicates are within the aggregate set
        self.enforce_unique_ids()

        # Continuation from interrupt/ start from scratch
        self.continuation()

        # Remove any NaN types from columns undergoing computation
        self.remove_na_working_columns()

        # Batching loop
        while self.batching():
            # Ensure that sighting dates follow the same format.
            self.format_observation_dates()

            # Generate country column from sighting coordinates
            # self.coordinate_to_country_rate_limited()

            # Generate local observation times
            self.generate_local_times()

            # Remove peripheral columns
            self.remove_peripheral_columns()

            # Write to interim data
            self.write_interim_data()

    def aggregate_observations(self):
        """Method aggregates all observations from separate files, placing them within a df for manipulation

        Method will check if dataframe is empty. This is to accommodate test cases which preloads the dataframe into
         the dataframe
        """
        if not self.TEST:
            for dataset in self.datasets:
                df_temp = pd.read_csv(self.resource_path + dataset)
                self.df_whole = pd.concat([self.df_whole, df_temp])

    def enforce_unique_ids(self):
        """Removal of any duplicate observations utilizing their observation id

        In place duplicate removal such that changes are effected directly within df
        """
        self.df_whole.drop_duplicates(subset=['id'], keep='first', inplace=True)

    def continuation(self, test_interim_df=None):
        self.df_whole.set_index('id', inplace=True)

        interim_df = pd.DataFrame()

        if self.TEST and test_interim_df is not None:
            interim_df = pd.concat([interim_df, test_interim_df])
        elif not self.TEST and self.interim_exists:
            interim_df = pd.read_csv(self.write_path + self.interim_file)

        if not interim_df.empty:
            interim_df.set_index('id', inplace=True)
            self.df_whole = self.df_whole.loc[self.df_whole.index.difference(interim_df.index),]

        self.row_sum = len(self.df_whole.index)

    def remove_na_working_columns(self):
        self.df_whole.dropna(subset=['observed_on', 'latitude', 'longitude', 'time_observed_at', 'time_zone'],
                             inplace=True)
        if self.df_whole.empty:
            print("*********** No further correctly format to process ***********")
            sys.exit()

    def batching(self) -> bool:
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

    # Inspiration: https://www.geeksforgeeks.org/progress-bars-in-python/
    def percentage(self, rows_remaining):
        progress_bar_length = 100
        percentage_complete = (self.row_sum - rows_remaining) / self.row_sum
        filled = int(progress_bar_length * percentage_complete)
        running_time = datetime.now() - self.start_time

        bar = '=' * filled + '-' * (progress_bar_length - filled)
        percentage_display = round(100 * percentage_complete, 1)
        sys.stdout.write('\r[%s] %s%s ... running: %s' %(bar, percentage_display, '%', running_time))
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

    # TODO Move into interim processing due to extreme time due to rate limiting
    # TODO Update documentation (explicitly rate limited)
    def coordinate_to_country_rate_limited(self):
        """ Method takes data coordinates, and identifies the country of origin, creating a Country column within Interim data

        The Geopy library is utilized, with rate limiting (1 sec) in order to not tax the API.
        """
        # Set up the geolocation library
        geolocator = Nominatim(user_agent="Spatio_Tempt_Class")
        geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)

        # Combine lat and long into coordinates
        latitudes = pd.Series(self.df.latitude.values.astype(str))
        longitudes = pd.Series(self.df.longitude.values.astype(str))
        coordinates = latitudes + ', ' + longitudes

        # Retrieve countries from coordinates (rate limiting requests)
        locations = coordinates.apply(partial(geocode, language='en', exactly_one=True))
        self.df['country'] = locations.apply(lambda x: x.raw['address']['country']).values

    # TODO Document method (not rate limited)
    def coordinate_to_country(self):
        # Set up the geolocation library
        geolocator = Nominatim(user_agent="Spatio_Tempt_Class")

        # Combine lat and long into coordinates
        latitudes = pd.Series(self.df.latitude.values.astype(str))
        longitudes = pd.Series(self.df.longitude.values.astype(str))
        coordinates = latitudes + ', ' + longitudes

        # Retrieve countries from coordinates (rate limiting requests)
        locations = coordinates.apply(partial(geolocator.reverse, language='en', exactly_one=True))
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

    def remove_peripheral_columns(self):
        self.df = self.df[['observed_on', 'local_time_observed_at', 'latitude', 'longitude',
                           'positional_accuracy', 'public_positional_accuracy', 'image_url', 'license', 'geoprivacy',
                           'taxon_geoprivacy', 'scientific_name', 'common_name', 'taxon_id']]

    def write_interim_data(self):
        """ Method writes current state of df into interim data folder in csv format"""
        if not self.TEST:
            if self.interim_exists:
                self.df.to_csv(self.write_path + self.interim_file, mode='a', index=True, header=False)
            else:
                self.df.to_csv(self.write_path + self.interim_file, mode='w', index=True, header=True)


if __name__ == "__main__":
    # Create Pipeline object
    pipeline = Pipeline(datasets=['observations_1.csv'])

    # Activate pipeline flow
    pipeline.activate_flow()
