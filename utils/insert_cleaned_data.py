import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from logging_setup import setup_logging

# Setup logger
logger = setup_logging('insert_cleaned_data')

# Load environment variables from .env file
load_dotenv()

def get_engine():
    """Create and return a database engine."""
    mysql_url = os.getenv("MYSQL_URL")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    cleaned_db = 'CleanedData'

    # Construct the connection URL
    connection_url = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_url}/{cleaned_db}"
    
    # Create and return the engine
    engine = create_engine(connection_url)
    return engine

def insert_data(engine, table_name, file_path):
    """Insert data from the file into the specified table."""
    logger.info(f'Inserting data from {file_path} into {table_name}...')
    df = pd.read_excel(file_path)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logger.info(f'Data from {file_path} successfully inserted into {table_name}.')

def main():
    cleaned_data_folder = os.path.join('data', 'processed_data')
    engine = get_engine()

    table_files = {
        'plaid_accounts': 'plaid_accounts.xlsx',
        'plaid_liabilities_credit': 'plaid_liabilities_credit.xlsx',
        'plaid_liabilities_credit_apr': 'plaid_liabilities_credit_apr.xlsx',
        'plaid_transactions': 'plaid_transactions.xlsx',
        'plaid_transaction_counterparties': 'plaid_transaction_counterparties.xlsx',
        'plaid_asset_report': 'asset_report.xlsx',
        'plaid_asset_item': 'asset_item.xlsx',
        'plaid_asset_account': 'asset_account.xlsx',
        'plaid_asset_transaction': 'asset_transaction.xlsx',
        'plaid_asset_historical_balance': 'asset_historical_balance.xlsx',
        'mbna_accounts': 'mbna_accounts.xlsx',
        'mbna_transactions': 'mbna_transactions.xlsx'
    }

    for table_name, file_name in table_files.items():
        file_path = os.path.join(cleaned_data_folder, file_name)
        if os.path.exists(file_path):
            insert_data(engine, table_name, file_path)
        else:
            logger.warning(f'File {file_path} does not exist. Skipping.')

if __name__ == "__main__":
    main()
