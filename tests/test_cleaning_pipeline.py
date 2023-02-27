import pandas as pd
import unittest

from src.data.DataCleanPipeline import Pipeline

# Test data retrieved from observations_1.csv and observations_6 (modified to include errors here)
# TODO Finish writing full test data
test_data = [
    [128984633, "2022-08-02", -30.4900714453, 151.6392706226, "2022-08-01 14:40:00 UTC", "Sydney", 'research', '',
     'https://www.inaturalist.org/observations/128984633',
     'https://static.inaturalist.org/photos/219142197/medium.jpeg', '', 11, 11, '', 'open', 'Phascolarctos cinereus',
     'Koala', 42983],
    [129051266, "2022-08-02", 43.1196234274, -7.6788841188, "2022-08-01 22:20:13 UTC", "Madrid", 'research', 'CC-BY',
     'https://www.inaturalist.org/observations/129051266',
     'https://inaturalist-open-data.s3.amazonaws.com/photos/219262307/medium.jpeg', '', 8, 8, '', 'open',
     'Plecotus auritus', 'Brown Big-eared Bat', 40416],
    [129054418, "2022-08/02", 50.6864393301, 7.1697807312, "2022-08-01 22:26:13 UTC", "Berlin"],
    [129076855, "2022-08-02", -40.9498116654, 174.9710916171, "2022-08-02 01:32:23 UTC", "Wellington"],
    [129076855, "2022-08-02", -40.9498116654, 174.9710916171, "2022-08-02 01:32:23 UTC", "Wellington"],
    [129107609, "202g-08-02", 43.952764223, -110.6115040714, "2022-08-02 07:14:59 UTC", "Mountain Time (US & Canada)"],
    [129120635, "2022-08-02", -18.83915, 16.9536, "2022-08-02 08:11:57 UTC", "Africa/Windhoek"],
    [38197744, "2020-02-02", -38.1974245434, 145.4793232007, "2020-02-01 23:04:35 UTC", "Asia/Magadan"],]

raw_data_columns = ['id', 'observed_on', 'latitude', 'longitude', 'time_observed_at', 'time_zone', 'quality_grade',
                    'license', 'url', 'image_url', 'description', 'positional_accuracy', 'public_positional_accuracy',
                    'geoprivacy', 'taxon_geoprivacy', 'scientific_name', 'common_name', 'taxon_id']

test_df = pd.DataFrame(test_data, columns=raw_data_columns)


class TestCleaningPipeline(unittest.TestCase):
    def setup(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.batch_size = 10
        pipeline.batching()
        return pipeline

    def test_unique_id(self):
        # Pipeline
        pipeline = self.setup()

        pipeline.enforce_unique_ids()
        resulting_ids = pipeline.df['id'].tolist()

        # Testing
        correct_ids = [128984633, 129051266, 129054418, 129076855, 129107609, 129120635, 38197744]
        self.assertTrue(resulting_ids.sort() == correct_ids.sort())

    def test_continuation(self):
        # Interim dataframe setup
        interim_data_columns = ['id', 'observed_on', 'local_time_observed_at', 'latitude', 'longitude',
                                'country', 'positional_accuracy', 'public_positional_accuracy', 'image_url',
                                'license', 'geoprivacy', 'taxon_geoprivacy', 'scientific_name', 'common_name',
                                'taxon_id']
        interim_data = [[128984633, '2022-08-02', '2022-08-02 00:40:00+10:00', -30.4900714453, 151.6392706226,
                         'Australia', 11, 11, 'https://static.inaturalist.org/photos/219142197/medium.jpeg', '', '',
                         'open', 'Phascolarctos cinereus', 'Koala', 42983],
                        [38197744, "2020-02-02", -38.1974245434, 145.4793232007, "2020-02-01 23:04:35 UTC", "Asia/Magadan"]]
        test_interim_df = pd.DataFrame(interim_data, columns=interim_data_columns)

        # Pipeline
        pipeline = Pipeline(test_df=test_df)
        pipeline.enforce_unique_ids()
        pipeline.continuation(test_interim_df=test_interim_df)
        indices_to_continue = pipeline.df_whole.index.to_list()

        # Testing
        correct_indices = [129051266, 129054418, 129076855, 129107609, 129120635]
        self.assertTrue(indices_to_continue.sort() == correct_indices.sort())

    def test_date_formatting(self):
        # Pipeline
        pipeline = self.setup()
        pipeline.format_observation_dates()
        resulting_formatted_dates = pipeline.df['observed_on'].tolist()

        # Testing
        correct_dates = ["2022-08-02", "2022-08-02", "2022-08-02",
                         "2022-08-02", "2022-08-02", "2020-02-02"]
        self.assertTrue(resulting_formatted_dates.sort() == correct_dates.sort())

    def test_coordinate_to_country(self):
        # Pipeline
        pipeline = self.setup()
        pipeline.coordinate_to_country()
        countries = pipeline.df['country'].values

        # Testing
        self.assertTrue(countries[0] == "Australia")
        self.assertTrue(countries[1] == "Spain")
        self.assertTrue(countries[2] == "Germany")
        self.assertTrue(countries[3] == "New Zealand")
        self.assertTrue(countries[5] == "United States")
        self.assertTrue(countries[6] == "Namibia")
        self.assertTrue(countries[7] == "Australia")

    def test_timezone_standardization(self):
        # Pipeline
        pipeline = self.setup()
        pipeline.standardize_timezones()
        timezones = pipeline.df['time_zone'].values

        # Testing
        self.assertTrue(timezones[0] == "Australia/Sydney")
        self.assertTrue(timezones[1] == "Europe/Madrid")
        self.assertTrue(timezones[2] == "Europe/Berlin")
        self.assertTrue(timezones[3] == "Pacific/Auckland")
        self.assertTrue(timezones[5] == "America/Denver")
        self.assertTrue(timezones[6] == "Africa/Windhoek")
        self.assertTrue(timezones[7] == "Australia/Melbourne")

    def test_local_times(self):
        # Pipeline
        pipeline = self.setup()
        pipeline.generate_local_times()
        local_times = pipeline.df['local_time_observed_at'].tolist()

        # Testing
        # Correct times confirmed using https://dateful.com/convert/utc
        correct_times = ["2022-08-02 00:40:00+10:00", "2022-08-02 00:20:13+02:00",
                         "2022-08-02 00:26:13+02:00", "2022-08-02 13:32:23+12:00",
                         "2022-08-02 13:32:23+12:00", "2022-08-02 01:14:59-06:00",
                         "2022-08-02 10:11:57+02:00", "2020-02-02 10:04:35+11:00"]
        self.assertTrue(set(local_times) == set(correct_times))

    def test_peripheral_column_removal(self):
        # Pipeline
        pipeline = Pipeline(test_df=test_df)
        pipeline.activate_flow()
        df_columns = pipeline.df.columns.tolist()

        # Testing
        correct_columns = ['observed_on', 'local_time_observed_at', 'latitude', 'longitude', 'country',
                           'positional_accuracy', 'public_positional_accuracy', 'image_url', 'license', 'geoprivacy',
                           'taxon_geoprivacy', 'scientific_name', 'common_name', 'taxon_id']
        self.assertTrue(set(df_columns) == set(correct_columns))


if __name__ == '__main__':
    unittest.main()

