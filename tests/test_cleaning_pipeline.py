import pandas as pd
import unittest

from src.data.DataCleanPipeline import Pipeline

test_data = [[128984633, "2022-08-02", -30.4900714453, 151.6392706226],
             [129048796, "2022-04-02", 25.594095, 85.137566],
             [110157830, "2022-04/02", -33.918861, 18.423300],
             [110266861, "2022-08-02", -38.6714966667, 178.0182195],
             [111311033, "202g-09-15", -31.416668, -64.183334],
             [111311033, "2022-08-15", 51.260197, 4.402771]]

test_df = pd.DataFrame(test_data, columns=['id',
                                           'observed_on',
                                           'latitude',
                                           'longitude'])


class TestCleaningPipeline(unittest.TestCase):
    # Test duplicate observation (unique id) removal
    def test_unique_id(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.enforce_unique_ids()

        correct_data = [[128984633, "2022-08-02"],
                        [129048796, "2022-04-02"],
                        [110157830, "2022-04/02"],
                        [110266861, "2022-08-02"],
                        [111311033, "202g-09-15"]]
        correct_df = pd.DataFrame(correct_data, columns=['id', 'observed_on'])

        self.assertTrue(correct_df.equals(pipeline.df))

    def test_data_formatting(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.format_observation_dates()

        correct_data = [[128984633, "2022-08-02"],
                        [129048796, "2022-04-02"],
                        [110266861, "2022-08-02"],
                        [111311033, "2022-08-15"]]
        correct_df = pd.DataFrame(correct_data, columns=['id', 'observed_on'])

        self.assertTrue(correct_df.equals(pipeline.df))

    def test_coordinate_to_country(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.coordinate_to_country()

        # Correct countries:
        # Australia, India, South Africa, New Zealand, Argentina, Belgium



if __name__ == '__main__':
    unittest.main()
