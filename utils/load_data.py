# utils/load_data.py
import pandas as pd
import logging
import numpy as np

def load_dataset(file_path):
    df = pd.read_excel(file_path)
    # Standardize transaction representation
    df['transaction_amount'] = df['transaction_amount'].abs()
    df['transaction_direction'] = np.where(df['is_transaction_outflow'] == 1, 'outflow', 'inflow')
    logging.info(f"Successfully loaded and standardized {len(df)} rows from {file_path}")
    return df