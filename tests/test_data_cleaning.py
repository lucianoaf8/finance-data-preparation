import unittest
import pandas as pd
from scripts.data_cleaning import handle_missing_values, correct_data_types

class TestDataCleaning(unittest.TestCase):
    def setUp(self):
        self.sample_data = pd.DataFrame({
            'transaction_amount': [100, None, 300],
            'transaction_date': ['2024-01-01', '2024-01-02', None],
            'category': ['A', None, 'C']
        })

    def test_handle_missing_values(self):
        cleaned_data = handle_missing_values(self.sample_data)
        self.assertFalse(cleaned_data.isnull().any().any())

    def test_correct_data_types(self):
        corrected_data = correct_data_types(self.sample_data)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(corrected_data['transaction_date']))
        self.assertTrue(pd.api.types.is_numeric_dtype(corrected_data['transaction_amount']))

# Add more test cases