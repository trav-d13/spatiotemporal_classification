import pandas as pd
import unittest

from src.data.DataCleanPipeline import Pipeline

# Test data retrieved from observations_1.csv (modified to include errors here)
test_data = [[128984633, "2022-08-02", -30.4900714453, 151.6392706226, "2022-08-01 14:40:00 UTC", "Sydney"],
             [129051266, "2022-08-02", 43.1196234274, -7.6788841188, "2022-08-01 22:20:13 UTC", "Madrid"],
             [129054418, "2022-08/02", 50.6864393301, 7.1697807312, "2022-08-01 22:26:13 UTC", "Berlin"],
             [129076855, "2022-08-02", -40.9498116654, 174.9710916171, "2022-08-02 01:32:23 UTC", "Wellington"],
             [129076855, "2022-08-02", -40.9498116654, 174.9710916171, "2022-08-02 01:32:23 UTC", "Wellington"],
             [129107609, "202g-08-15", 43.952764223, -110.6115040714, "2022-08-02 07:14:59 UTC", "Mountain Time (US & Canada)"],
             [129120635, "2022-08-15", -18.83915, 16.9536, "2022-08-02 08:11:57 UTC", "Africa/Windhoek"]]

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

        resulting_ids = pipeline.df['id'].tolist()
        correct_ids = [128984633, 129051266, 129054418, 129076855, 129107609, 129120635]

        self.assertTrue(resulting_ids == correct_ids)

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
