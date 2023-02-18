import pandas as pd
import unittest

from src.data.DataCleanPipeline import Pipeline

# Test data retrieved from observations_1.csv and observations_6 (modified to include errors here)
test_data = [[128984633, "2022-08-02", -30.4900714453, 151.6392706226, "2022-08-01 14:40:00 UTC", "Sydney"],
             [129051266, "2022-08-02", 43.1196234274, -7.6788841188, "2022-08-01 22:20:13 UTC", "Madrid"],
             [129054418, "2022-08/02", 50.6864393301, 7.1697807312, "2022-08-01 22:26:13 UTC", "Berlin"],
             [129076855, "2022-08-02", -40.9498116654, 174.9710916171, "2022-08-02 01:32:23 UTC", "Wellington"],
             [129076855, "2022-08-02", -40.9498116654, 174.9710916171, "2022-08-02 01:32:23 UTC", "Wellington"],
             [129107609, "202g-08-02", 43.952764223, -110.6115040714, "2022-08-02 07:14:59 UTC", "Mountain Time (US & Canada)"],
             [129120635, "2022-08-02", -18.83915, 16.9536, "2022-08-02 08:11:57 UTC", "Africa/Windhoek"],
             [38197744, "2020-02-02", -38.1974245434, 145.4793232007, "2020-02-01 23:04:35 UTC", "Asia/Magadan"]]

test_df = pd.DataFrame(test_data, columns=['id',
                                           'observed_on',
                                           'latitude',
                                           'longitude',
                                           'time_observed_at',
                                           'time_zone'])


class TestCleaningPipeline(unittest.TestCase):
    def test_unique_id(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.enforce_unique_ids()

        resulting_ids = pipeline.df['id'].tolist()
        correct_ids = [128984633, 129051266, 129054418, 129076855, 129107609, 129120635, 38197744]

        self.assertTrue(resulting_ids == correct_ids)

    def test_date_formatting(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.format_observation_dates()

        resulting_formatted_dates = pipeline.df['observed_on'].tolist()
        print(resulting_formatted_dates)
        correct_dates = ["2022-08-02", "2022-08-02", "2022-08-02",
                         "2022-08-02", "2022-08-02", "2020-02-02"]

        self.assertTrue(resulting_formatted_dates == correct_dates)

    def test_coordinate_to_country(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.coordinate_to_country()
        countries = pipeline.df['country']

        self.assertTrue(countries[0] == "Australia")
        self.assertTrue(countries[1] == "Spain")
        self.assertTrue(countries[2] == "Germany")
        self.assertTrue(countries[3] == "New Zealand")
        self.assertTrue(countries[5] == "United States")
        self.assertTrue(countries[6] == "Namibia")
        self.assertTrue(countries[7] == "Australia")

    def test_timezone_standardization(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.standardize_timezones()
        timezones = pipeline.df['time_zone']

        self.assertTrue(timezones[0] == "Australia/Sydney")
        self.assertTrue(timezones[1] == "Europe/Madrid")
        self.assertTrue(timezones[2] == "Europe/Berlin")
        self.assertTrue(timezones[3] == "Pacific/Auckland")
        self.assertTrue(timezones[5] == "America/Denver")
        self.assertTrue(timezones[6] == "Africa/Windhoek")
        self.assertTrue(timezones[7] == "Australia/Melbourne")

    def test_local_times(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.generate_local_times()

        local_times = pipeline.df['local_time_observed_at'].tolist()
        print(type(local_times[0]))

        # Correct times confirmed using https://dateful.com/convert/utc
        correct_times = ["2022-08-02 00:40:00+10:00", "2022-08-02 00:20:13+02:00",
                         "2022-08-02 00:26:13+02:00", "2022-08-02 13:32:23+12:00",
                         "2022-08-02 13:32:23+12:00", "2022-08-02 01:14:59-06:00",
                         "2022-08-02 10:11:57+02:00", "2020-02-02 10:04:35+11:00"]


if __name__ == '__main__':
    unittest.main()
