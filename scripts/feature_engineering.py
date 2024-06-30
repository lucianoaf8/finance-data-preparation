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

def add_row_features(df, table_name):
    current_date = pd.to_datetime('now')
    if table_name == 'plaid_accounts':
        df['balance_to_limit_ratio'] = df['current_balance'] / df['balance_limit']
        df['available_to_current_balance_ratio'] = df['available_balance'] / df['current_balance']
        df['account_age'] = (current_date - pd.to_datetime(df['created_at'])).dt.days

    elif table_name == 'plaid_liabilities_credit':
        df['days_since_last_payment'] = (current_date - pd.to_datetime(df['last_payment_date'])).dt.days
        df['days_since_last_statement'] = (current_date - pd.to_datetime(df['last_statement_issue_date'])).dt.days
        df['days_to_next_payment_due'] = (pd.to_datetime(df['next_payment_due_date']) - current_date).dt.days
        df['is_recently_overdue'] = df['days_since_last_payment'] <= 30

    elif table_name == 'plaid_liabilities_credit_apr':
        df['interest_cost_per_dollar'] = df['apr_percentage'] * df['balance_subject_to_apr'] / 100
        df['annual_interest_cost'] = df['balance_subject_to_apr'] * df['apr_percentage'] / 100

    elif table_name == 'plaid_transactions':
        df['transaction_hour'] = pd.to_datetime(df['datetime']).dt.hour
        df['transaction_day_of_week'] = pd.to_datetime(df['datetime']).dt.dayofweek
        df['transaction_month'] = pd.to_datetime(df['datetime']).dt.month

    elif table_name == 'asset_report':
        df['report_age'] = (current_date - pd.to_datetime(df['date_generated'])).dt.days
        df['days_requested_normalized'] = (df['days_requested'] - df['days_requested'].mean()) / df['days_requested'].std()

    elif table_name == 'asset_item':
        df['days_since_last_update'] = (current_date - pd.to_datetime(df['date_last_updated'])).dt.days

    elif table_name == 'asset_account':
        df['balance_to_limit_ratio'] = df['current'] / df['limit']
        df['available_to_current_balance_ratio'] = df['available'] / df['current']
        df['account_age'] = (current_date - pd.to_datetime(df['created_at'])).dt.days

    elif table_name == 'asset_transaction':
        df['transaction_hour'] = pd.to_datetime(df['date']).dt.hour
        df['transaction_day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
        df['transaction_month'] = pd.to_datetime(df['date']).dt.month

    elif table_name == 'asset_historical_balance':
        df['rolling_avg_7d'] = df['current'].rolling(window=7).mean()
        df['rolling_avg_30d'] = df['current'].rolling(window=30).mean()
        df['balance_volatility_7d'] = df['current'].rolling(window=7).std()
        df['balance_volatility_30d'] = df['current'].rolling(window=30).std()

    elif table_name == 'mbna_accounts':
        df['credit_utilization_ratio'] = df['credit_available'] / df['credit_limit']
        df['cash_advance_utilization_ratio'] = df['cash_advance_available'] / df['cash_advance_limit']
        df['account_age'] = (current_date - pd.to_datetime(df['statement_closing_date'])).dt.days

    elif table_name == 'mbna_transactions':
        df['transaction_hour'] = pd.to_datetime(df['posting_date']).dt.hour
        df['transaction_day_of_week'] = pd.to_datetime(df['posting_date']).dt.dayofweek
        df['transaction_month'] = pd.to_datetime(df['posting_date']).dt.month

    return df

def create_aggregate_features(df, table_name, output_folder_path):
    if table_name == 'plaid_transactions':
        agg_features = df.groupby('account_id').agg(
            transaction_count=pd.NamedAgg(column='transaction_id', aggfunc='count'),
            total_amount=pd.NamedAgg(column='amount', aggfunc='sum'),
            avg_transaction_amount=pd.NamedAgg(column='amount', aggfunc='mean'),
            max_transaction_amount=pd.NamedAgg(column='amount', aggfunc='max'),
            min_transaction_amount=pd.NamedAgg(column='amount', aggfunc='min'),
            pending_transaction_count=pd.NamedAgg(column='pending', aggfunc='sum')
        ).reset_index()
        save_featured_file(agg_features, 'added_feature_transactions_aggregate.xlsx', output_folder_path)

    elif table_name == 'asset_transaction':
        agg_features = df.groupby('account_id').agg(
            transaction_count=pd.NamedAgg(column='transaction_id', aggfunc='count'),
            total_interest_paid=pd.NamedAgg(column='amount', aggfunc=lambda x: x[x > 0].sum()),
            avg_transaction_amount=pd.NamedAgg(column='amount', aggfunc='mean'),
            max_transaction_amount=pd.NamedAgg(column='amount', aggfunc='max'),
            min_transaction_amount=pd.NamedAgg(column='amount', aggfunc='min')
        ).reset_index()
        save_featured_file(agg_features, 'added_feature_asset_transactions_aggregate.xlsx', output_folder_path)

    elif table_name == 'plaid_transactions':
        # Spending Categories
        categories = ['shopping', 'groceries']  # Example categories
        for category in categories:
            category_col = f'total_{category}_spent'
            df[category_col] = df.apply(lambda row: row['amount'] if row['category'] == category else 0, axis=1)
        agg_features = df.groupby('account_id').agg(
            **{f'total_{category}_spent': pd.NamedAgg(column=f'total_{category}_spent', aggfunc='sum') for category in categories}
        ).reset_index()
        save_featured_file(agg_features, 'added_feature_transactions_category_spending.xlsx', output_folder_path)

def process_files(input_folder_path, output_folder_path):
    dataframes = load_excel_files(input_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)
    
    for file, df in dataframes.items():
        table_name = file.split('.')[0]  # assuming the table name is the filename without extension
        logger.info(f'Adding features to {file}')
        df = add_row_features(df, table_name)
        
        # Save the enhanced dataframe back to Excel
        save_featured_file(df, f'{table_name}.xlsx', output_folder_path)

        # Create and save aggregated features if applicable
        create_aggregate_features(df, table_name, output_folder_path)

if __name__ == '__main__':
    input_folder_path = 'data/dupes_removed_data'
    output_folder_path = 'data/featured_added_data'
    process_files(input_folder_path, output_folder_path)
