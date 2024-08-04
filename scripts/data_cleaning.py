# scripts/data_cleaning.py
import pandas as pd
import numpy as np
import logging
from sklearn.impute import KNNImputer

def handle_missing_values(df):
    try:
        # Identify columns with missing data
        missing_data = df.isnull().sum()
        logging.info("Columns with missing data:")
        for col, count in missing_data[missing_data > 0].items():
            logging.info(f"{col}: {count}")
        
        # Fill numeric columns with median
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
        
        # Fill categorical columns with mode, converting to string first
        categorical_columns = df.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            df[col] = df[col].astype(str).fillna(df[col].astype(str).mode().iloc[0])
        
        logging.info("Missing values handled successfully")
        return df
    except Exception as e:
        logging.error(f"Error handling missing values: {str(e)}")
        raise

def correct_data_types(df):
    """
    Correct data types for date and numeric fields.
    """
    try:
        # Convert date strings to datetime objects
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        # Ensure numeric fields are in appropriate formats
        numeric_columns = ['transaction_amount', 'account_current_balance', 'account_limit']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                logging.warning(f"Column '{col}' not found in the DataFrame. Skipping conversion.")
        
        logging.info("Data types corrected successfully")
        return df
    except Exception as e:
        logging.error(f"Error correcting data types: {str(e)}")
        raise

def standardize_categories(df):
    """
    Standardize merchant names and transaction categories.
    """
    try:
        # Unify merchant names
        df['merchant_name'] = df['merchant_name'].str.upper()
        df['merchant_name'] = df['merchant_name'].replace({
            'UBER': 'UBER',
            'UBER*TRIP': 'UBER',
            # Add more replacements as needed
        })
        
        # Standardize transaction categories (example, modify as per your categories)
        category_mapping = {
            'FOOD_AND_DRINK_RESTAURANT': 'FOOD_AND_DRINK',
            'FOOD_AND_DRINK_FAST_FOOD': 'FOOD_AND_DRINK',
            # Add more mappings as needed
        }
        df['personal_finance_category_primary'] = df['personal_finance_category_primary'].replace(category_mapping)
        
        logging.info("Categories standardized successfully")
        return df
    except Exception as e:
        logging.error(f"Error standardizing categories: {str(e)}")
        raise
    
def advanced_imputation(df):
    # Separate numeric and categorical columns
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    categorical_columns = df.select_dtypes(include=['object']).columns

    # KNN imputation for numeric columns
    imputer = KNNImputer(n_neighbors=5)
    df[numeric_columns] = imputer.fit_transform(df[numeric_columns])

    # Mode imputation for categorical columns
    for col in categorical_columns:
        df[col] = df[col].fillna(df[col].mode().iloc[0])

    return df

# Call this function in handle_missing_values