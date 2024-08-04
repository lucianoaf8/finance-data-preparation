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

def process_files(input_folder_path, output_folder_path):
    dataframes = load_excel_files(input_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)
    
    for file, df in dataframes.items():
        table_name = os.path.splitext(file)[0]
        logger.info(f'Processing file: {table_name}')

if __name__ == '__main__':
    input_folder_path = 'data_files/dupes_removed'
    output_folder_path = 'data_files/feature_added'
    process_files(input_folder_path, output_folder_path)
