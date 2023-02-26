import pandas as pd
import os

from datetime import datetime
from timezonefinder import TimezoneFinder

import pytz

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

from functools import partial

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
            self.interim_file = "interim_observations.csv"
            self.TEST = False
        else:
            self.df = test_df.copy(deep=True)
            self.TEST = True

    def activate_flow(self):
        # Aggregate all observation files
        self.aggregate_observations()

        # Ensure that no sighting duplicates are within the aggregate set
        self.enforce_unique_ids()

        # Continuation from interrupt/ start from scratch

        # Ensure that sighting dates follow the same format.
        self.format_observation_dates()

        # Generate country column from sighting coordinates
        self.coordinate_to_country()

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
        for dataset in self.datasets:
            df_temp = pd.read_csv(self.resource_path + dataset)
            self.df = pd.concat([self.df, df_temp])

    def enforce_unique_ids(self):
        """Removal of any duplicate observations utilizing their observation id

        In place duplicate removal such that changes are effected directly within df
        """
        self.df.drop_duplicates(subset=['id'], keep='first', inplace=True)

    def continuation(self, test_interim_df=None):
        self.df.set_index('id', inplace=True)

        interim_df = pd.DataFrame()

        if self.TEST and test_interim_df is not None:
            interim_df = test_interim_df.copy(deep=True).set_index('id', inplace=True)
        else:
            interim_data_flag = os.path.isfile(self.write_path + self.interim_file)
        elif interim_data_flag:
            interim_df = pd.read_csv(self.write_path + self.interim_file).set_index('id', inplace=True)

        if not interim_df.empty:
            self.df = self.df.loc[self.df.index.difference(interim_df.index),]
            print(self.df.head())

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

    def coordinate_to_country(self):
        """ Method takes data coordinates, and identifies the country of origin, creating a Country column within Interim data

        The Geopy library is utilized, with rate limiting (1 sec) in order to not tax the API.
        """
        # Set up the geolocation library
        geolocator = Nominatim(user_agent="Spatio_Tempt_Class")
        geocode = RateLimiter(geolocator.reverse, min_delay_seconds=0.2)

        # Combine lat and long into coordinates
        latitudes = self.df.latitude.astype(str)
        longitudes = self.df.longitude.astype(str)
        coordinates = latitudes + ", " + longitudes

        # Retrieve countries from coordinates (rate limiting requests)
        locations = coordinates.apply(partial(geocode, language='en', exactly_one=True))
        self.df['country'] = locations.apply(lambda x: x.raw['address']['country'])

    def generate_local_times(self):
        """ Method converts UTC time to correct local time utilizing the specified sighting timezone.

        The new column generated is labelled "local_time_observed_at" and can be found in interim data folder.

        The date conversion utilizes both date and time in the UTC format.
        This means, any day change (midnight -> next day) are accounted for.
        The observed_on column is correct.
        """
        # Remove any rows with empty values
        self.df.dropna(subset=['time_observed_at', 'time_zone'], inplace=True)
        self.df.reset_index(drop=True, inplace=True)

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
        self.df = self.df[['id', 'observed_on', 'local_time_observed_at', 'latitude', 'longitude', 'country',
                           'positional_accuracy', 'public_positional_accuracy', 'image_url', 'license', 'geoprivacy',
                           'taxon_geoprivacy', 'scientific_name', 'common_name', 'taxon_id']]

    def write_interim_data(self):
        """ Method writes current state of df into interim data folder in csv format"""
        if not self.TEST:
            self.df.to_csv(self.write_path + self.interim_file, index=True)


if __name__ == "__main__":
    # Create Pipeline object
    pipeline = Pipeline()

    # Activate pipeline flow
    pipeline.activate_flow()
