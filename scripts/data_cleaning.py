# scripts/data_cleaning.py

import os
import sys
import pandas as pd

# Add the project root to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logging_setup import setup_logging

# Setup logger
logger = setup_logging('data_cleaning')

def load_excel_files(folder_path):
    files = os.listdir(folder_path)
    return {file: pd.read_excel(os.path.join(folder_path, file)) for file in files if file.endswith('.xlsx')}

def save_cleaned_file(df, file_name, folder_path):
    output_path = os.path.join(folder_path, file_name)
    df.to_excel(output_path, index=False)
    logger.info(f'Saved cleaned data to {output_path}')

def clean_currency_code(code):
    valid_codes = ['USD', 'CAD', 'BRL']
    if code not in valid_codes:
        return 'CAD'
    return code

def clean_dates(df, date_columns):
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    return df

def clean_numeric(df, numeric_columns):
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def clean_strings(df, string_columns):
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()
    return df

def clean_plaid_accounts(df):
    df = clean_strings(df, ['bank_name', 'name', 'official_name', 'type', 'subtype'])
    df = clean_numeric(df, ['available_balance', 'current_balance', 'balance_limit'])
    df['iso_currency_code'] = df['iso_currency_code'].str.upper().apply(clean_currency_code)
    df['unofficial_currency_code'] = df['unofficial_currency_code'].apply(lambda x: None if pd.isnull(x) or len(x) > 10 else x)
    df['mask'] = df['mask'].astype(str).str.zfill(4)
    df = clean_dates(df, ['created_at', 'updated_at'])
    return df

def clean_plaid_liabilities_credit(df):
    df = clean_strings(df, ['account_id'])
    if 'is_overdue' in df.columns:
        df['is_overdue'] = df['is_overdue'].astype(int).astype(bool)
    numeric_columns = ['last_payment_amount', 'last_statement_balance', 'minimum_payment_amount']
    numeric_columns = [col for col in numeric_columns if col in df.columns]
    df = clean_numeric(df, numeric_columns)
    date_columns = ['last_payment_date', 'last_statement_issue_date', 'next_payment_due_date']
    date_columns = [col for col in date_columns if col in df.columns]
    df = clean_dates(df, date_columns)
    return df

def clean_plaid_liabilities_credit_apr(df):
    df = clean_strings(df, ['account_id', 'apr_type'])
    df = clean_numeric(df, ['apr_percentage', 'balance_subject_to_apr', 'interest_charge_amount'])
    return df

def clean_plaid_transactions(df):
    df = clean_strings(df, [
        'account_id', 'transaction_id', 'account_owner', 'merchant_entity_id', 'merchant_name', 'name', 'payment_channel', 
        'pending_transaction_id', 'transaction_code', 'transaction_type', 'category', 'category_id', 
        'personal_finance_category_confidence_level', 'personal_finance_category_detailed', 
        'personal_finance_category_primary', 'location_address', 'location_city', 'location_region', 
        'location_postal_code', 'location_country', 'location_store_number', 'payment_meta_reference_number', 
        'payment_meta_ppd_id', 'payment_meta_payee', 'payment_meta_by_order_of', 'payment_meta_payer', 
        'payment_meta_payment_method', 'payment_meta_payment_processor', 'payment_meta_reason'
    ])
    df = clean_numeric(df, ['amount', 'location_lat', 'location_lon'])
    df['iso_currency_code'] = df['iso_currency_code'].str.upper().apply(clean_currency_code)
    df['unofficial_currency_code'] = df['unofficial_currency_code'].apply(lambda x: None if pd.isnull(x) or len(x) > 10 else x)
    df = clean_dates(df, ['authorized_date', 'authorized_datetime', 'date', 'datetime'])
    df['pending'] = df['pending'].astype(bool)
    return df

def clean_plaid_transaction_counterparties(df):
    df = clean_strings(df, ['transaction_id', 'name', 'type', 'website', 'logo_url', 'confidence_level', 'entity_id', 'phone_number'])
    return df

def clean_categories(df):
    df = clean_strings(df, ['category_group', 'hierarchy_level1', 'hierarchy_level2', 'hierarchy_level3'])
    return df

def clean_asset_report(df):
    df = clean_strings(df, ['asset_report_id', 'client_report_id'])
    df = clean_dates(df, ['date_generated', 'created_at'])
    df['days_requested'] = df['days_requested'].apply(lambda x: x if x > 0 else None)
    df['file_path'] = df['file_path'].apply(lambda x: x if pd.notnull(x) and len(x) <= 255 else None)
    df['json_file'] = df['json_file'].apply(lambda x: x if pd.notnull(x) else '{}')
    return df

def clean_asset_item(df):
    df = clean_strings(df, ['institution_name', 'item_id', 'asset_report_id'])
    df = clean_dates(df, ['date_last_updated'])
    return df

def clean_asset_account(df):
    df = clean_strings(df, ['account_id', 'name', 'official_name', 'type', 'subtype', 'item_id', 'asset_report_id'])
    df = clean_numeric(df, ['available', 'current', 'limit', 'margin_loan_amount'])
    df['iso_currency_code'] = df['iso_currency_code'].str.upper().apply(clean_currency_code)
    df['unofficial_currency_code'] = df['unofficial_currency_code'].apply(lambda x: None if pd.isnull(x) or len(x) > 10 else x)
    return df

def clean_asset_transaction(df):
    df = clean_strings(df, ['transaction_id', 'account_id', 'original_description', 'asset_report_id'])
    df = clean_numeric(df, ['amount'])
    df['iso_currency_code'] = df['iso_currency_code'].str.upper().apply(clean_currency_code)
    df['unofficial_currency_code'] = df['unofficial_currency_code'].apply(lambda x: None if pd.isnull(x) or len(x) > 10 else x)
    df = clean_dates(df, ['date'])
    return df

def clean_asset_historical_balance(df):
    df = clean_strings(df, ['account_id', 'asset_report_id'])
    df = clean_numeric(df, ['current'])
    df['iso_currency_code'] = df['iso_currency_code'].str.upper().apply(clean_currency_code)
    df['unofficial_currency_code'] = df['unofficial_currency_code'].apply(lambda x: None if pd.isnull(x) or len(x) > 10 else x)
    df = clean_dates(df, ['date'])
    return df

def clean_mbna_accounts(df):
    df = clean_strings(df, ['cardholder_name', 'account_number'])
    df = clean_numeric(df, ['credit_limit', 'cash_advance_limit', 'credit_available', 'cash_advance_available'])
    df = clean_dates(df, ['statement_closing_date'])
    df['annual_interest_rate_purchases'] = df['annual_interest_rate_purchases'].apply(lambda x: x if 0 <= x <= 100 else None)
    df['annual_interest_rate_balance_transfers'] = df['annual_interest_rate_balance_transfers'].apply(lambda x: x if 0 <= x <= 100 else None)
    df['annual_interest_rate_cash_advances'] = df['annual_interest_rate_cash_advances'].apply(lambda x: x if 0 <= x <= 100 else None)
    return df

def clean_mbna_transactions(df):
    df = clean_strings(df, ['payeee', 'adrdress'])  # Use the original column names here
    df.rename(columns={'payeee': 'payee', 'adrdress': 'address'}, inplace=True)  # Rename columns after cleaning
    df = clean_numeric(df, ['amount'])
    df = clean_dates(df, ['posting_date'])
    return df

def clean_data(input_folder_path, output_folder_path):
    dataframes = load_excel_files(input_folder_path)
    
    for file, df in dataframes.items():
        if 'plaid_accounts' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_plaid_accounts(df)
        elif 'plaid_liabilities_credit' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_plaid_liabilities_credit(df)
        elif 'plaid_liabilities_credit_apr' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_plaid_liabilities_credit_apr(df)
        elif 'plaid_transactions' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_plaid_transactions(df)
        elif 'plaid_transaction_counterparties' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_plaid_transaction_counterparties(df)
        elif 'categories' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_categories(df)
        elif 'asset_report' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_asset_report(df)
        elif 'asset_item' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_asset_item(df)
        elif 'asset_account' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_asset_account(df)
        elif 'asset_transaction' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_asset_transaction(df)
        elif 'asset_historical_balance' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_asset_historical_balance(df)
        elif 'mbna_accounts' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_mbna_accounts(df)
        elif 'mbna_transactions' in file:
            logger.info(f'Cleaning {file}')
            dataframes[file] = clean_mbna_transactions(df)

        # Save the cleaned dataframe back to Excel
        save_cleaned_file(dataframes[file], file, output_folder_path)

if __name__ == '__main__':
    input_folder_path = 'data_files/fetched'
    output_folder_path = 'data_files/cleaned'
    os.makedirs(output_folder_path, exist_ok=True)
    clean_data(input_folder_path, output_folder_path)
