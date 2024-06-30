# scripts/data_fetcher.py

import os
import pandas as pd
from sqlalchemy import create_engine
from utils.logging_setup import setup_logging
from utils.db_connection import get_engine

# Setup logger
logger = setup_logging('data_fetcher')

def fetch_data(engine, table_name):
    file_path = os.path.join('data', 'fetched_data', f'{table_name}.xlsx')
    
    if os.path.exists(file_path):
        logger.info(f'Loading data from existing file {file_path}...')
        df = pd.read_excel(file_path)
    else:
        logger.info(f'Fetching data from {table_name}...')
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql(query, engine)
        df.to_excel(file_path, index=False)
        logger.info(f'Data from {table_name} saved to {file_path}.')
    
    return df

def fetch_all_data(engine, tables):
    dataframes = {}
    for table in tables:
        dataframes[table] = fetch_data(engine, table)
    return dataframes

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    
    plaid_db = os.getenv('PLAID_DB')
    finance_db = os.getenv('MBNA_DB')

    plaid_engine = get_engine(plaid_db)
    finance_engine = get_engine(finance_db)

    plaid_tables = [
        'plaid_accounts', 'plaid_liabilities_credit', 'plaid_liabilities_credit_apr',
        'plaid_transactions', 'plaid_transaction_counterparties', 'asset_report',
        'asset_item', 'asset_account', 'asset_transaction', 'asset_historical_balance'
    ]
    finance_tables = ['mbna_accounts', 'mbna_transactions']

    fetch_all_data(plaid_engine, plaid_tables)
    fetch_all_data(finance_engine, finance_tables)
