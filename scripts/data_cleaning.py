# utils/data_cleaning.py

import os
import pandas as pd
from utils.logging_setup import setup_logging
import numpy as np

# Setup logger
logger = setup_logging('data_cleaning')

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
    df.loc[df[column].isnull(), column] = default_value
    return df

def standardize_addresses(df, address_columns):
    for column in address_columns:
        if column in df.columns and df[column].dtype == 'object':
            logger.info(f'Standardizing address column {column}...')
            df[column] = df[column].str.strip().str.title()
    return df

def format_dates(df, date_columns):
    for column in date_columns:
        if column in df.columns:
            logger.info(f'Formatting date column {column}...')
            df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y-%m-%d')
            df[column] = df[column].replace('NaT', np.nan)
    return df

def ensure_data_types(df, column_types):
    for column, dtype in column_types.items():
        if column in df.columns:
            logger.info(f'Ensuring data type for column {column} to be {dtype}...')
            try:
                df[column] = pd.to_numeric(df[column]) if dtype == 'float' else df[column].astype(dtype)
            except (ValueError, TypeError) as e:
                logger.warning(f'Could not convert column {column} to {dtype}: {e}')
    return df

def check_referential_integrity(df, foreign_keys):
    for key, reference_df in foreign_keys.items():
        if key in df.columns and key in reference_df.columns:
            logger.info(f'Checking referential integrity for key {key}...')
            df = df[df[key].isin(reference_df[key])]
    return df

def add_derived_columns(df, derived_columns):
    for new_column, logic in derived_columns.items():
        logger.info(f'Adding derived column {new_column}...')
        df[new_column] = df.apply(logic, axis=1)
    return df

def clean_data(df, table_name):
    logger.info(f'Starting cleaning process for {table_name}...')

    # Date formatting and consistency
    date_columns = ['created_at', 'updated_at', 'last_payment_date', 'last_statement_issue_date', 'next_payment_due_date', 'authorized_date', 'date', 'datetime', 'posting_date']
    df = format_dates(df, date_columns)

    # Data type consistency
    column_types = {
        'amount': 'float',
        'balance_limit': 'float',
        'current_balance': 'float',
        'available_balance': 'float',
        'iso_currency_code': 'str'
    }
    df = ensure_data_types(df, column_types)

    # Standardize address and location data
    address_columns = ['location_address', 'location_city', 'location_postal_code']
    df = standardize_addresses(df, address_columns)

    # Example for plaid_accounts
    if table_name == 'plaid_accounts':
        df = remove_duplicates(df, 'account_id')
        df = handle_missing_values(df, 'balance_limit', 0)
        df = handle_missing_values(df, 'iso_currency_code', 'CAD')

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
        df = handle_missing_values(df, 'iso_currency_code', 'CAD')
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
        df = handle_missing_values(df, 'limit', 0)
        df = handle_missing_values(df, 'iso_currency_code', 'CAD')

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

    # Referential integrity checks (example)
    if table_name == 'plaid_transactions':
        plaid_accounts_df = pd.read_excel(os.path.join('data', 'fetched_data', 'plaid_accounts.xlsx'))
        df = check_referential_integrity(df, {'account_id': plaid_accounts_df})

    # Add derived columns (example)
    if table_name == 'plaid_transactions':
        df = add_derived_columns(df, {'day_of_week': lambda row: pd.Timestamp(row['date']).day_name()})

    logger.info(f'Finished cleaning process for {table_name}.')
    return df

def clean_all_data(dataframes, tables):
    for table in tables:
        df = dataframes[table]
        cleaned_df = clean_data(df, table)
        save_cleaned_data(cleaned_df, table)
