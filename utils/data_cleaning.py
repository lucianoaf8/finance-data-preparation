# utils/data_cleaning.py

import os
import pandas as pd
from utils.logging_setup import setup_logging

# Setup logger
logger = setup_logging('data_cleaning')

def load_data(table_name):
    file_path = os.path.join('data', 'fetched_data', f'{table_name}.xlsx')
    if os.path.exists(file_path):
        logger.info(f'Loading data from {file_path}...')
        df = pd.read_excel(file_path)
    else:
        raise FileNotFoundError(f"No data file found for {table_name} at {file_path}")
    return df

def save_cleaned_data(df, table_name):
    file_path = os.path.join('data', 'processed_data', f'{table_name}.xlsx')
    df.to_excel(file_path, index=False)
    logger.info(f'Cleaned data saved to {file_path}.')

def remove_duplicates(df, unique_column):
    logger.info(f'Removing duplicates based on {unique_column}...')
    df = df.drop_duplicates(subset=[unique_column])
    return df

def handle_missing_values(df, column, default_value):
    logger.info(f'Handling missing values in {column}...')
    df[column] = df[column].fillna(default_value)
    return df

def clean_data(df, table_name):
    logger.info(f'Starting cleaning process for {table_name}...')
    # Implement specific cleaning logic for each table here

    # Example for plaid_accounts
    if table_name == 'plaid_accounts':
        df = remove_duplicates(df, 'account_id')
        df = handle_missing_values(df, 'balance_limit', 0)
        df = handle_missing_values(df, 'iso_currency_code', 'USD')

    # Add specific cleaning steps for other tables here...

    # plaid_liabilities_credit table
    if table_name == 'plaid_liabilities_credit':
        df = remove_duplicates(df, 'account_id')
        df = handle_missing_values(df, 'last_payment_amount', 0)
        df = handle_missing_values(df, 'last_statement_balance', 0)
        df = handle_missing_values(df, 'minimum_payment_amount', 0)

    # plaid_liabilities_credit_apr table
    if table_name == 'plaid_liabilities_credit_apr':
        df = remove_duplicates(df, 'account_id')
        df = handle_missing_values(df, 'apr_percentage', 0)
        df = handle_missing_values(df, 'balance_subject_to_apr', 0)
        df = handle_missing_values(df, 'interest_charge_amount', 0)

    # plaid_transactions table
    if table_name == 'plaid_transactions':
        df = remove_duplicates(df, 'transaction_id')
        df = handle_missing_values(df, 'amount', 0)
        df = handle_missing_values(df, 'iso_currency_code', 'USD')
        df = handle_missing_values(df, 'location_lat', 0)
        df = handle_missing_values(df, 'location_lon', 0)

    # plaid_transaction_counterparties table
    if table_name == 'plaid_transaction_counterparties':
        df = remove_duplicates(df, 'transaction_id')
        df = handle_missing_values(df, 'name', 'Unknown')
        df = handle_missing_values(df, 'type', 'Unknown')
        df = handle_missing_values(df, 'confidence_level', 'low')

    # asset_report table
    if table_name == 'asset_report':
        df = remove_duplicates(df, 'asset_report_id')
        df = handle_missing_values(df, 'json_file', '{}')

    # asset_item table
    if table_name == 'asset_item':
        df = remove_duplicates(df, 'item_id')

    # asset_account table
    if table_name == 'asset_account':
        df = remove_duplicates(df, 'account_id')
        df = handle_missing_values(df, 'available', 0)
        df = handle_missing_values(df, 'current', 0)
        df = handle_missing_values(df, 'iso_currency_code', 'USD')

    # asset_transaction table
    if table_name == 'asset_transaction':
        df = remove_duplicates(df, 'transaction_id')
        df = handle_missing_values(df, 'amount', 0)

    # asset_historical_balance table
    if table_name == 'asset_historical_balance':
        df = remove_duplicates(df, 'balance_id')
        df = handle_missing_values(df, 'current', 0)

    # mbna_accounts table
    if table_name == 'mbna_accounts':
        df = remove_duplicates(df, 'id')
        df = handle_missing_values(df, 'credit_limit', 0)
        df = handle_missing_values(df, 'cash_advance_limit', 0)
        df = handle_missing_values(df, 'credit_available', 0)
        df = handle_missing_values(df, 'cash_advance_available', 0)
        df = handle_missing_values(df, 'annual_interest_rate_purchases', 0)
        df = handle_missing_values(df, 'annual_interest_rate_balance_transfers', 0)
        df = handle_missing_values(df, 'annual_interest_rate_cash_advances', 0)

    # mbna_transactions table
    if table_name == 'mbna_transactions':
        df = remove_duplicates(df, 'transaction_id')
        df = handle_missing_values(df, 'amount', 0)

    save_cleaned_data(df, table_name)
    logger.info(f'Finished cleaning process for {table_name}.')
    return df

def clean_all_data(tables):
    for table in tables:
        df = load_data(table)
        clean_data(df, table)

if __name__ == '__main__':
    plaid_tables = [
        'plaid_accounts', 'plaid_liabilities_credit', 'plaid_liabilities_credit_apr',
        'plaid_transactions', 'plaid_transaction_counterparties', 'asset_report',
        'asset_item', 'asset_account', 'asset_transaction', 'asset_historical_balance'
    ]
    finance_tables = ['mbna_accounts', 'mbna_transactions']

    clean_all_data(plaid_tables)
    clean_all_data(finance_tables)
