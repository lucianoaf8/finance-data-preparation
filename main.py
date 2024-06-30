# main.py

from utils.db_connection import get_engine
from utils.data_cleaning import clean_all_data
from utils.data_fetcher import fetch_all_data
import os
from dotenv import load_dotenv
from utils.logging_setup import setup_logging

# Load environment variables from .env file
load_dotenv()

# Setup logger
logger = setup_logging('main')

def main():
    try:
        plaid_db = os.getenv('PLAID_DB')
        finance_db = os.getenv('MBNA_DB')
        
        logger.info('Starting data fetching process...')
        
        # Connect to the databases
        logger.info('Connecting to Plaid database...')
        plaid_engine = get_engine(plaid_db)
        logger.info('Connecting to Finance database...')
        finance_engine = get_engine(finance_db)
        
        # Define tables to fetch and clean
        plaid_tables = [
            'plaid_accounts', 'plaid_liabilities_credit', 'plaid_liabilities_credit_apr',
            'plaid_transactions', 'plaid_transaction_counterparties', 'asset_report',
            'asset_item', 'asset_account', 'asset_transaction', 'asset_historical_balance'
        ]
        finance_tables = ['mbna_accounts', 'mbna_transactions']

        # Fetch data from both databases
        logger.info('Fetching data from Plaid database...')
        plaid_dataframes = fetch_all_data(plaid_engine, plaid_tables)
        logger.info('Fetching data from Finance database...')
        finance_dataframes = fetch_all_data(finance_engine, finance_tables)
        
        logger.info('Starting data cleaning process...')
        
        # Clean data from both databases
        clean_all_data(plaid_dataframes, plaid_tables)
        clean_all_data(finance_dataframes, finance_tables)
        
        logger.info('Data cleaning process completed successfully.')
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == '__main__':
    main()
