# scripts/data_validation.py
import pandas as pd
import logging

def validate_data(df):
    """
    Perform sanity checks on prepared data.
    """
    try:
        validation_results = {}
        
        # Check for negative balances
        validation_results['negative_balances'] = (df['account_current_balance'] < 0).sum()
        
        # Verify date ranges
        validation_results['min_date'] = df['transaction_date'].min()
        validation_results['max_date'] = df['transaction_date'].max()
        
        # Check for missing values
        validation_results['missing_values'] = df.isnull().sum().sum()
        
        # Verify data consistency
        validation_results['unique_accounts'] = df['account_id'].nunique()
        validation_results['total_transactions'] = len(df)
        
        logging.info("Data validation completed successfully")
        return validation_results
    except Exception as e:
        logging.error(f"Error in data validation: {str(e)}")
        raise