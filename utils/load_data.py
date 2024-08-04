# utils/load_data.py
import pandas as pd
import logging

def load_dataset(file_path):
    """
    Load the dataset from the Excel file.
    """
    try:
        df = pd.read_excel(file_path)
        logging.info(f"Successfully loaded {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {str(e)}")
        raise