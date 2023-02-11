import pandas as pd
import unittest

from src.data.DataCleanPipeline import Pipeline

test_data = [[128984633, "2022-08-02"],
             [129048796, "2022-04-02"],
             [110157830, "2022-04-02"],
             [110266861, "2022-08-02"],
             [111311033, "2022-09-15"],
             [111311033, "2022-09-15"]]

correct_data = [[128984633, "2022-08-02"],
                [129048796, "2022-04-02"],
                [110157830, "2022-04-02"],
                [110266861, "2022-08-02"],
                [111311033, "2022-09-15"]]

test_df = pd.DataFrame(test_data, columns=['id', 'observed_on'])
correct_df = pd.DataFrame(correct_data, columns=['id', 'observed_on'])


class TestCleaningPipeline(unittest.TestCase):
    # Test duplicate observation (unique id) removal
    def test_unique_id(self):
        pipeline = Pipeline(test_df=test_df)
        pipeline.enforce_unique_ids()
        self.assertTrue(correct_df.equals(pipeline.df))


if __name__ == '__main__':
    unittest.main()
