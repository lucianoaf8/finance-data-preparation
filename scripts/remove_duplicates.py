# scripts/remove_duplicates.py

import os
import sys
import pandas as pd

# Add the project root to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logging_setup import setup_logging

# Setup logger
logger = setup_logging('remove_duplicates')

def load_excel_files(folder_path):
    files = os.listdir(folder_path)
    return {file: pd.read_excel(os.path.join(folder_path, file)) for file in files if file.endswith('.xlsx')}

def save_cleaned_file(df, file_name, folder_path):
    output_path = os.path.join(folder_path, file_name)
    df.to_excel(output_path, index=False)
    logger.info(f'Saved cleaned data to {output_path}')

def remove_duplicates(df, subset):
    initial_count = len(df)
    df.drop_duplicates(subset=subset, keep='first', inplace=True)
    final_count = len(df)
    removed_count = initial_count - final_count
    return df, removed_count

def process_files(input_folder_path, output_folder_path):
    dataframes = load_excel_files(input_folder_path)
    report_data = []

    for file, df in dataframes.items():
        if 'plaid_accounts' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['account_id'])
        elif 'plaid_liabilities_credit' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['account_id'])
        elif 'plaid_liabilities_credit_apr' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['account_id', 'apr_type'])
        elif 'plaid_transactions' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['transaction_id'])
        elif 'plaid_transaction_counterparties' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['transaction_id', 'entity_id'])
        elif 'categories' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['category_id'])
        elif 'asset_report' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['asset_report_id'])
        elif 'asset_item' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['item_id'])
        elif 'asset_account' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['account_id'])
        elif 'asset_transaction' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['transaction_id'])
        elif 'asset_historical_balance' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['account_id', 'date'])
        elif 'mbna_accounts' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['account_number'])
        elif 'mbna_transactions' in file:
            logger.info(f'Removing duplicates from {file}')
            dataframes[file], removed_count = remove_duplicates(df, ['account_id', 'transaction_id'])

        # Save the deduplicated dataframe back to Excel
        save_cleaned_file(dataframes[file], f'deduped_{file}', output_folder_path)
        
        # Add to report data
        report_data.append([file, removed_count])

    # Save the report
    report_df = pd.DataFrame(report_data, columns=['File', 'Rows Removed'])
    report_output_path = os.path.join('reports', 'remove_dupes_report.xlsx')
    os.makedirs(os.path.dirname(report_output_path), exist_ok=True)
    report_df.to_excel(report_output_path, index=False)
    logger.info(f'Report saved to {report_output_path}')

if __name__ == '__main__':
    input_folder_path = 'data/cleaned_data'
    output_folder_path = 'data/dupes_removed_data'
    os.makedirs(output_folder_path, exist_ok=True)
    process_files(input_folder_path, output_folder_path)
