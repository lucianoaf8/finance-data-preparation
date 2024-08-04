# scripts/feature_engineering.py
import pandas as pd
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, StandardScaler
import logging

def create_derived_features(df):
    try:
        # Day of week for transactions
        df['transaction_day_of_week'] = df['transaction_date'].dt.dayofweek
        
        # Month-end account balances (assuming the data is sorted by date)
        df['month_end'] = df['transaction_date'].dt.is_month_end
        df['month_end_balance'] = df.loc[df['month_end'], 'account_current_balance']
        df['month_end_balance'] = df['month_end_balance'].ffill()  # Use ffill() instead of fillna(method='ffill')
        
        # Transaction frequency per merchant/category
        df['merchant_frequency'] = df.groupby('merchant_name')['transaction_id'].transform('count')
        df['category_frequency'] = df.groupby('personal_finance_category_primary')['transaction_id'].transform('count')
        
        logging.info("Derived features created successfully")
        return df
    
    except Exception as e:
        logging.error(f"Error creating derived features: {str(e)}")
        raise
    
def encode_categorical_variables(df):
    try:
        # One-hot encoding for account types and transaction categories
        onehot_columns = ['account_type', 'personal_finance_category_primary']
        onehot_encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        onehot_encoded = onehot_encoder.fit_transform(df[onehot_columns])
        onehot_columns_names = onehot_encoder.get_feature_names_out(onehot_columns)
        onehot_df = pd.DataFrame(onehot_encoded, columns=onehot_columns_names, index=df.index)
        
        # Label encoding for merchants
        le = LabelEncoder()
        df['merchant_encoded'] = le.fit_transform(df['merchant_name'])
        
        # Combine the original dataframe with the one-hot encoded features
        df = pd.concat([df, onehot_df], axis=1)
        
        logging.info("Categorical variables encoded successfully")
        return df
    except Exception as e:
        logging.error(f"Error encoding categorical variables: {str(e)}")
        raise
    
def normalize_numerical_features(df):
    """
    Normalize numerical features using StandardScaler.
    """
    try:
        numeric_columns = ['transaction_amount', 'account_current_balance', 'account_limit']
        columns_to_normalize = [col for col in numeric_columns if col in df.columns]
        
        if columns_to_normalize:
            scaler = StandardScaler()
            df[columns_to_normalize] = scaler.fit_transform(df[columns_to_normalize])
            logging.info("Numerical features normalized successfully")
        else:
            logging.warning("No numeric columns found for normalization")
        
        return df
    except Exception as e:
        logging.error(f"Error normalizing numerical features: {str(e)}")
        raise

def create_advanced_features(df):
    # Rolling averages
    df['7day_avg'] = df.groupby('account_id')['transaction_amount'].rolling(window=7).mean().reset_index(0, drop=True)
    df['30day_avg'] = df.groupby('account_id')['transaction_amount'].rolling(window=30).mean().reset_index(0, drop=True)

    # Day of week and time of month features
    df['day_of_week'] = df['transaction_date'].dt.dayofweek
    df['day_of_month'] = df['transaction_date'].dt.day

    # Spending ratios
    category_totals = df.groupby('personal_finance_category_primary')['transaction_amount'].transform('sum')
    df['category_spending_ratio'] = df['transaction_amount'] / category_totals

    return df

# Call this function in the main pipeline