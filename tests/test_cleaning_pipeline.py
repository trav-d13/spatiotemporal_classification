import pandas as pd
import unittest

from src.data.DataCleanPipeline import Pipeline

test_data = [[128984633, "2022-08-02", -30.4900714453, 151.6392706226, "2022-08-01 14:40:00 UTC", "Australia/Sydney"],
             [129048796, "2022-04-02", 25.594095, 85.137566, "2022-08-01 22:20:13 UTC", "Madrid"],
             [110157830, "2022-04/02", -33.918861, 18.423300, "2022-08-01 22:26:13 UTC", "Berlin"],
             [110266861, "2022-08-02", -38.6714966667, 178.0182195, "2022-08-02 08:11:57 UTC", "Africa/Windhoek"],
             [111311033, "202g-09-15", -31.416668, -64.183334, "2022-08-02 11:37:59 UTC", "Eastern Time (US & Canada)"],
             [111311033, "2022-08-15", 51.260197, 4.402771, "2022-08-02 09:42:33 UTC", "London"]]

test_df = pd.DataFrame(test_data, columns=['id',
                                           'observed_on',
                                           'latitude',
                                           'longitude',
                                           'time_observed_at',
                                           'time_zone'])


class TestCleaningPipeline(unittest.TestCase):
    # Test duplicate observation (unique id) removal
    def test_unique_id(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.enforce_unique_ids()

        correct_data = [[128984633, "2022-08-02", -30.4900714453, 151.6392706226],
                        [129048796, "2022-04-02", 25.594095, 85.137566],
                        [110157830, "2022-04/02", -33.918861, 18.423300],
                        [110266861, "2022-08-02", -38.6714966667, 178.0182195],
                        [111311033, "202g-09-15", -31.416668, -64.183334]]
        correct_df = pd.DataFrame(correct_data, columns=['id', 'observed_on', 'latitude', 'longitude'])

        self.assertTrue(correct_df.equals(pipeline.df))

    def test_data_formatting(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.format_observation_dates()

        correct_data = [[128984633, "2022-08-02", -30.4900714453, 151.6392706226],
                        [129048796, "2022-04-02", 25.594095, 85.137566],
                        [110266861, "2022-08-02", -38.6714966667, 178.0182195],
                        [111311033, "2022-08-15", 51.260197, 4.402771]]
        correct_df = pd.DataFrame(correct_data, columns=['id', 'observed_on', 'latitude', 'longitude'])

        self.assertTrue(correct_df.equals(pipeline.df))

    def test_coordinate_to_country(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.coordinate_to_country()
        countries = pipeline.df['country']
        print(countries)

        self.assertTrue(countries[0] == "Australia")
        self.assertTrue(countries[1] == "India")
        self.assertTrue(countries[2] == "South Africa")
        self.assertTrue(countries[3] == "New Zealand")
        self.assertTrue(countries[4] == "Argentina")
        self.assertTrue(countries[5] == "Belgium")

    def test_local_times(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.generate_local_times()

        # 2022/08/02 12:40 AM -> Sydney
        # 2022-08-02 00:20:13 -> Madrid
        # 2022-08-02 10:11:57+02:00 -> Africa/Windhoek
        # 2022-08-02 07:37:59-04:00 -> Eastern Time (US & Canada)
        # 2022-08-02 10:42:33 -> London

    def test_timezone_standardization(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.standardize_timezones()
        timezones = pipeline.df['time_zone']
        print(timezones)

        self.assertTrue(timezones[0] == "Australia/Sydney")


if __name__ == '__main__':
    unittest.main()
