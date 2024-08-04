# main.py
import os
import sys
import logging
from datetime import datetime

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from utils.load_data import load_dataset
from scripts.data_consolidation import consolidate_data
from scripts.data_cleaning import handle_missing_values, correct_data_types, standardize_categories
from scripts.feature_engineering import create_derived_features, encode_categorical_variables, normalize_numerical_features
from scripts.time_series_preparation import prepare_time_series
from scripts.anomaly_detection import detect_anomalies
from scripts.data_validation import validate_data

def setup_logging():
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"data_preparation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def main():
    setup_logging()
    logging.info("Starting data preparation process")

    try:
        # Load data
        file_path = os.path.join(project_root, "data_files", "base_all_accounts_transactions_Jan24-July24.xlsx")
        df = load_dataset(file_path)
        logging.info("Data loaded successfully")

        # Data preparation steps
        steps = [
            ("Data consolidation", consolidate_data),
            ("Handling missing values", handle_missing_values),
            ("Correcting data types", correct_data_types),
            ("Standardizing categories", standardize_categories),
            ("Creating derived features", create_derived_features),
            ("Encoding categorical variables", encode_categorical_variables),
            ("Normalizing numerical features", normalize_numerical_features),
            ("Preparing time series", prepare_time_series),
            ("Detecting anomalies", detect_anomalies)
        ]

        for step_name, step_function in steps:
            logging.info(f"Starting {step_name}")
            df = step_function(df)
            logging.info(f"{step_name} completed successfully")

        # Data validation
        logging.info("Starting data validation")
        validation_results = validate_data(df)
        for key, value in validation_results.items():
            logging.info(f"Validation - {key}: {value}")

        # Save processed data
        output_dir = os.path.join(project_root, "database")
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "processed_data.xlsx")
        df.to_excel(output_file, index=False)
        logging.info(f"Processed data saved to {output_file}")

        logging.info("Data preparation process completed successfully")

    except Exception as e:
        logging.error(f"An error occurred during data preparation: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()