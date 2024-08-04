# scripts/data_consolidation.py
import pandas as pd
import logging

def consolidate_data(df):
    """
    Ensure consistent column naming and merge all data into a single dataset.
    """
    try:
        # Rename columns to ensure consistency (if needed)
        column_mapping = {
            'account_name': 'account_name',
            'transaction_date': 'transaction_date',
            'transaction_amount': 'amount',
            # Add more mappings as needed
        }
        df = df.rename(columns=column_mapping)
        
        # If there are multiple sheets or dataframes, merge them here
        # For now, we'll assume all data is already in a single dataframe
        
        logging.info(f"Data consolidated successfully. Shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error during data consolidation: {str(e)}")
        raise