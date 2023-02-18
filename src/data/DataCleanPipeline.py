import pandas as pd

from datetime import datetime
from timezonefinder import TimezoneFinder

import pytz
from dateutil.tz import tzutc
from pytz import timezone

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

    # TODO Generate acceptable time-zones from geopy as get:
    # pytz.exceptions.UnknownTimeZoneError: 'Sydney'
    def generate_local_times(self):
        # Remove any rows with empty values
        self.df.dropna(subset=['time_observed_at', 'time_zone'], inplace=True)
        self.df.reset_index(drop=True, inplace=True)

        # Generate timezones such that they are acceptable to pytz

        # Access time_observed at column (UTC format) and local time zones
        self.df['local_time_observed_at'] = self.df.apply(
            lambda x: pd.to_datetime(x['time_observed_at'], utc=True).astimezone(pytz.timezone(x['time_zone'])), axis=1)
        print(self.df)

    def standardize_timezones(self):
        finder = TimezoneFinder()

        self.df['time_zone'] = self.df.apply(
            lambda x: finder.timezone_at(lat=x['latitude'], lng=x['longitude']), axis=1)

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

    # Generate country column from sighting coordinates
    pipeline.coordinate_to_country()

    # Write to interim data
    pipeline.write_interim_data()

    # Display df head
    print(pipeline.df.head())
