import os
import pandas as pd
from sqlalchemy import create_engine, inspect, exc
from dotenv import load_dotenv
from logging_setup import setup_logging
from db_connection import get_engine

# Setup logger
logger = setup_logging('insert_cleaned_data')

# Load environment variables from .env file
load_dotenv()

def log_column_discrepancies(df_columns, table_columns, table_name):
    """Log discrepancies between DataFrame columns and table schema."""
    df_columns_set = set(df_columns)
    table_columns_set = set(table_columns)

    columns_in_df_not_in_table = df_columns_set - table_columns_set
    columns_in_table_not_in_df = table_columns_set - df_columns_set

    if columns_in_df_not_in_table:
        logger.warning(f"Columns in DataFrame but not in table '{table_name}': {columns_in_df_not_in_table}")
    if columns_in_table_not_in_df:
        logger.warning(f"Columns in table '{table_name}' but not in DataFrame: {columns_in_table_not_in_df}")

def filter_columns(df, engine, table_name):
    """Filter DataFrame columns to match the table schema and log discrepancies."""
    inspector = inspect(engine)
    table_columns = [col['name'] for col in inspector.get_columns(table_name)]
    
    log_column_discrepancies(df.columns, table_columns, table_name)
    
    filtered_columns = [col for col in table_columns if col in df.columns]
    filtered_df = df[filtered_columns]
    return filtered_df

def remove_duplicates(df, primary_key):
    """Remove duplicates based on the primary key."""
    return df.drop_duplicates(subset=[primary_key])

def insert_data(engine, table_name, file_path, primary_key):
    """Insert data from the file into the specified table."""
    logger.info(f'Inserting data from {file_path} into {table_name}...')
    
    try:
        df = pd.read_excel(file_path)
        
        # Remove duplicates based on the primary key
        df = remove_duplicates(df, primary_key)
        
        # Filter columns to match the table schema and log discrepancies
        df_filtered = filter_columns(df, engine, table_name)
        
        df_filtered.to_sql(table_name, con=engine, if_exists='append', index=False)
        logger.info(f'Data from {file_path} successfully inserted into {table_name}.')
    except exc.IntegrityError as e:
        logger.error(f'Integrity error while inserting data from {file_path} into {table_name}: {e.orig}')
    except exc.OperationalError as e:
        logger.error(f'Operational error while inserting data from {file_path} into {table_name}: {e.orig}')
    except exc.SQLAlchemyError as e:
        logger.error(f'SQLAlchemy error while inserting data from {file_path} into {table_name}: {e}')
    except Exception as e:
        logger.error(f'Unexpected error while inserting data from {file_path} into {table_name}: {e}')

def main():
    enriched_data_folder = os.path.join('data', 'enriched_data')
    
    # Use get_engine function from db_connection.py to get the engine for CleanedData database
    engine = get_engine('CleanedData')

    table_files = {
        'plaid_transactions_enriched': ('plaid_transactions_enriched.xlsx', 'id'),
        'plaid_transaction_counterparties_enriched': ('plaid_transaction_counterparties_enriched.xlsx', 'id'),
        'asset_account_enriched': ('asset_account_enriched.xlsx', 'account_id'),
        'asset_historical_balance_enriched': ('asset_historical_balance_enriched.xlsx', 'balance_id'),
        'asset_item_enriched': ('asset_item_enriched.xlsx', 'item_id'),
        'asset_report_enriched': ('asset_report_enriched.xlsx', 'asset_report_id'),
        'asset_transaction_enriched': ('asset_transaction_enriched.xlsx', 'transaction_id'),
        'mbna_accounts_enriched': ('mbna_accounts_enriched.xlsx', 'id'),
        'mbna_transactions_enriched': ('mbna_transactions_enriched.xlsx', 'transaction_id'),
        'plaid_accounts_enriched': ('plaid_accounts_enriched.xlsx', 'account_id'),
        'plaid_liabilities_credit_apr_enriched': ('plaid_liabilities_credit_apr_enriched.xlsx', 'id'),
        'plaid_liabilities_credit_enriched': ('plaid_liabilities_credit_enriched.xlsx', 'id')
    }

    for table_name, (file_name, primary_key) in table_files.items():
        file_path = os.path.join(enriched_data_folder, file_name)
        if os.path.exists(file_path):
            insert_data(engine, table_name, file_path, primary_key)
        else:
            logger.warning(f'File {file_path} does not exist. Skipping.')

if __name__ == "__main__":
    main()
