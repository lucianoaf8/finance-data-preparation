# scripts/feature_engineering.py

import os
import sys
import pandas as pd

# Add the project root to the PYTHONPATH to ensure the script can find the utils module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logging_setup import setup_logging

# Setup logger
logger = setup_logging('feature_engineering')

def load_excel_files(folder_path):
    files = os.listdir(folder_path)
    return {file: pd.read_excel(os.path.join(folder_path, file)) for file in files if file.endswith('.xlsx')}

def save_featured_file(df, file_name, folder_path):
    output_path = os.path.join(folder_path, file_name)
    df.to_excel(output_path, index=False)
    logger.info(f'Saved featured data to {output_path}')

def log_column_names(df, table_name):
    logger.info(f'Columns in {table_name}: {list(df.columns)}')

def add_plaid_accounts_features(df):
    required_columns = ['current_balance', 'balance_limit', 'available_balance']
    if all(col in df.columns for col in required_columns):
        df['balance_to_limit_ratio'] = df['current_balance'] / df['balance_limit']
        df['available_to_current_balance_ratio'] = df['available_balance'] / df['current_balance']
    else:
        logger.warning(f'Missing columns in plaid_accounts: {set(required_columns) - set(df.columns)}')
    return df

def add_plaid_liabilities_credit_features(df):
    required_columns = []
    # No features based on today's date for plaid_liabilities_credit
    return df

def add_plaid_liabilities_credit_apr_features(df):
    required_columns = ['apr_percentage', 'balance_subject_to_apr']
    if all(col in df.columns for col in required_columns):
        df['interest_cost_per_dollar'] = df['apr_percentage'] * df['balance_subject_to_apr'] / 100
        df['annual_interest_cost'] = df['balance_subject_to_apr'] * df['apr_percentage'] / 100
    else:
        logger.warning(f'Missing columns in plaid_liabilities_credit_apr: {set(required_columns) - set(df.columns)}')
    return df

def add_plaid_transactions_features(df):
    required_columns = ['datetime']
    if all(col in df.columns for col in required_columns):
        df['transaction_hour'] = pd.to_datetime(df['datetime']).dt.hour
        df['transaction_day_of_week'] = pd.to_datetime(df['datetime']).dt.dayofweek
        df['transaction_month'] = pd.to_datetime(df['datetime']).dt.month
    else:
        logger.warning(f'Missing columns in plaid_transactions: {set(required_columns) - set(df.columns)}')
    return df

def add_asset_report_features(df):
    required_columns = ['days_requested']
    if all(col in df.columns for col in required_columns):
        df['days_requested_normalized'] = (df['days_requested'] - df['days_requested'].mean()) / df['days_requested'].std()
    else:
        logger.warning(f'Missing columns in asset_report: {set(required_columns) - set(df.columns)}')
    return df

def add_asset_item_features(df):
    required_columns = []
    # No features based on today's date for asset_item
    return df

def add_asset_account_features(df):
    required_columns = ['current', 'limit', 'available']
    if all(col in df.columns for col in required_columns):
        df['balance_to_limit_ratio'] = df['current'] / df['limit']
        df['available_to_current_balance_ratio'] = df['available'] / df['current']
    else:
        logger.warning(f'Missing columns in asset_account: {set(required_columns) - set(df.columns)}')
    return df

def add_asset_transaction_features(df):
    required_columns = ['date']
    if all(col in df.columns for col in required_columns):
        df['transaction_hour'] = pd.to_datetime(df['date']).dt.hour
        df['transaction_day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
        df['transaction_month'] = pd.to_datetime(df['date']).dt.month
    else:
        logger.warning(f'Missing columns in asset_transaction: {set(required_columns) - set(df.columns)}')
    return df

def add_asset_historical_balance_features(df):
    required_columns = ['current']
    if all(col in df.columns for col in required_columns):
        df['rolling_avg_7d'] = df['current'].rolling(window=7).mean()
        df['rolling_avg_30d'] = df['current'].rolling(window=30).mean()
        df['balance_volatility_7d'] = df['current'].rolling(window=7).std()
        df['balance_volatility_30d'] = df['current'].rolling(window=30).std()
    else:
        logger.warning(f'Missing columns in asset_historical_balance: {set(required_columns) - set(df.columns)}')
    return df

def add_mbna_accounts_features(df):
    required_columns = ['credit_available', 'credit_limit', 'cash_advance_available', 'cash_advance_limit']
    if all(col in df.columns for col in required_columns):
        df['credit_utilization_ratio'] = df['credit_available'] / df['credit_limit']
        df['cash_advance_utilization_ratio'] = df['cash_advance_available'] / df['cash_advance_limit']
    else:
        logger.warning(f'Missing columns in mbna_accounts: {set(required_columns) - set(df.columns)}')
    return df

def add_mbna_transactions_features(df):
    required_columns = ['posting_date']
    if all(col in df.columns for col in required_columns):
        df['transaction_hour'] = pd.to_datetime(df['posting_date']).dt.hour
        df['transaction_day_of_week'] = pd.to_datetime(df['posting_date']).dt.dayofweek
        df['transaction_month'] = pd.to_datetime(df['posting_date']).dt.month
    else:
        logger.warning(f'Missing columns in mbna_transactions: {set(required_columns) - set(df.columns)}')
    return df

def add_row_features(df, table_name):
    log_column_names(df, table_name)  # Log column names for debugging
    if table_name == 'deduped_cleaned_plaid_accounts':
        return add_plaid_accounts_features(df)
    elif table_name == 'deduped_cleaned_plaid_liabilities_credit':
        return add_plaid_liabilities_credit_features(df)
    elif table_name == 'deduped_cleaned_plaid_liabilities_credit_apr':
        return add_plaid_liabilities_credit_apr_features(df)
    elif table_name == 'deduped_cleaned_plaid_transactions':
        return add_plaid_transactions_features(df)
    elif table_name == 'deduped_cleaned_asset_report':
        return add_asset_report_features(df)
    elif table_name == 'deduped_cleaned_asset_item':
        return add_asset_item_features(df)
    elif table_name == 'deduped_cleaned_asset_account':
        return add_asset_account_features(df)
    elif table_name == 'deduped_cleaned_asset_transaction':
        return add_asset_transaction_features(df)
    elif table_name == 'deduped_cleaned_asset_historical_balance':
        return add_asset_historical_balance_features(df)
    elif table_name == 'deduped_cleaned_mbna_accounts':
        return add_mbna_accounts_features(df)
    elif table_name == 'deduped_cleaned_mbna_transactions':
        return add_mbna_transactions_features(df)
    else:
        logger.warning(f'No feature engineering implemented for {table_name}')
        return df

def create_aggregate_features(df, table_name, output_folder_path):
    if table_name == 'deduped_cleaned_plaid_transactions':
        agg_features = df.groupby('account_id').agg(
            transaction_count=pd.NamedAgg(column='transaction_id', aggfunc='count'),
            total_amount=pd.NamedAgg(column='amount', aggfunc='sum'),
            avg_transaction_amount=pd.NamedAgg(column='amount', aggfunc='mean'),
            max_transaction_amount=pd.NamedAgg(column='amount', aggfunc='max'),
            min_transaction_amount=pd.NamedAgg(column='amount', aggfunc='min'),
            pending_transaction_count=pd.NamedAgg(column='pending', aggfunc='sum')
        ).reset_index()
        save_featured_file(agg_features, 'added_feature_transactions_aggregate.xlsx', output_folder_path)

    elif table_name == 'deduped_cleaned_asset_transaction':
        agg_features = df.groupby('account_id').agg(
            transaction_count=pd.NamedAgg(column='transaction_id', aggfunc='count'),
            total_interest_paid=pd.NamedAgg(column='amount', aggfunc=lambda x: x[x > 0].sum()),
            avg_transaction_amount=pd.NamedAgg(column='amount', aggfunc='mean'),
            max_transaction_amount=pd.NamedAgg(column='amount', aggfunc='max'),
            min_transaction_amount=pd.NamedAgg(column='amount', aggfunc='min')
        ).reset_index()
        save_featured_file(agg_features, 'added_feature_asset_transactions_aggregate.xlsx', output_folder_path)

def process_files(input_folder_path, output_folder_path):
    dataframes = load_excel_files(input_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)
    
    for file, df in dataframes.items():
        table_name = os.path.splitext(file)[0]  # removing the .xlsx extension
        logger.info(f'Processing file: {table_name}')
        df = add_row_features(df, table_name)
        
        # Save the enhanced dataframe back to Excel
        save_featured_file(df, f'{table_name}.xlsx', output_folder_path)

        # Create and save aggregated features if applicable
        create_aggregate_features(df, table_name, output_folder_path)

if __name__ == '__main__':
    input_folder_path = 'data_files/dupes_removed'
    output_folder_path = 'data_files/feature_added'
    process_files(input_folder_path, output_folder_path)
