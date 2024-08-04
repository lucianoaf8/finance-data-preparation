# scripts/time_series_preparation.py
import pandas as pd
import logging

def prepare_time_series(df):
    """
    Prepare data for time series analysis.
    """
    try:
        # Sort data chronologically
        df = df.sort_values('transaction_date')
        
        # Create lag features
        df['prev_transaction_amount'] = df.groupby('account_id')['transaction_amount'].shift(1)
        
        # Generate rolling statistics (7-day average spending)
        df['rolling_7day_avg'] = df.groupby('account_id')['transaction_amount'].rolling(window=7).mean().reset_index(0, drop=True)
        
        logging.info("Time series preparation completed successfully")
        return df
    except Exception as e:
        logging.error(f"Error in time series preparation: {str(e)}")
        raise