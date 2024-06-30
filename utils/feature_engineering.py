# utils/feature_engineering.py

import os
import pandas as pd
import numpy as np
from logging_setup import setup_logging

# Setup logger
logger = setup_logging('feature_engineering')

def load_data(file_name):
    file_path = os.path.join('data', 'processed_data', file_name)
    return pd.read_excel(file_path)

def save_data(df, file_name):
    file_path = os.path.join('data', 'enriched_data', file_name)
    df.to_excel(file_path, index=False)
    logger.info(f'Enriched data saved to {file_path}.')

def add_temporal_features(df, date_column):
    if date_column in df.columns:
        logger.info('Adding temporal features...')
        df['transaction_hour'] = pd.to_datetime(df[date_column]).dt.hour
        df['transaction_day_of_week'] = pd.to_datetime(df[date_column]).dt.day_name()
        df['transaction_week_of_year'] = pd.to_datetime(df[date_column]).dt.isocalendar().week
        df['transaction_month'] = pd.to_datetime(df[date_column]).dt.month
        df['transaction_quarter'] = pd.to_datetime(df[date_column]).dt.quarter
    return df

def add_recency_features(df, date_column, id_column):
    if date_column in df.columns:
        logger.info('Adding recency features...')
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        df['days_since_last_transaction'] = df.groupby(id_column)[date_column].diff().dt.days.fillna(0)
    return df

def add_account_level_aggregates(df, id_column, amount_column):
    if id_column in df.columns:
        logger.info('Adding account-level aggregates...')
        aggregates = df.groupby(id_column).agg(
            total_transactions=(amount_column, 'count'),
            total_amount_spent=(amount_column, 'sum'),
            average_transaction_amount=(amount_column, 'mean'),
            max_transaction_amount=(amount_column, 'max')
        ).reset_index()
        df = df.merge(aggregates, on=id_column, how='left')
    return df

def add_monthly_aggregates(df, date_column, id_column, amount_column):
    if date_column in df.columns:
        logger.info('Adding monthly aggregates...')
        df['year_month'] = pd.to_datetime(df[date_column]).dt.to_period('M')
        monthly_aggregates = df.groupby([id_column, 'year_month']).agg(
            monthly_total_spent=(amount_column, 'sum'),
            monthly_transaction_count=(amount_column, 'count'),
            monthly_average_transaction_amount=(amount_column, 'mean')
        ).reset_index()
        df = df.merge(monthly_aggregates, on=[id_column, 'year_month'], how='left')
    return df

def add_behavioral_features(df, id_column, merchant_column, category_column, amount_column):
    logger.info('Adding behavioral features...')
    if merchant_column in df.columns:
        most_frequent_merchant = df.groupby(id_column)[merchant_column].agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan).reset_index()
        df = df.merge(most_frequent_merchant, on=id_column, how='left', suffixes=('', '_most_frequent'))
    if category_column in df.columns:
        most_frequent_category = df.groupby(id_column)[category_column].agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan).reset_index()
        df = df.merge(most_frequent_category, on=id_column, how='left', suffixes=('', '_most_frequent'))
    spending_variance = df.groupby(id_column)[amount_column].var().reset_index(name='spending_variance')
    df = df.merge(spending_variance, on=id_column, how='left')
    return df

def add_category_features(df, category_column, amount_column):
    if category_column in df.columns:
        logger.info('Adding category features...')
        category_aggregates = df.groupby(category_column).agg(
            total_spent_per_category=(amount_column, 'sum'),
            average_spent_per_category=(amount_column, 'mean'),
            transaction_count_per_category=(amount_column, 'count')
        ).reset_index()
        df = df.merge(category_aggregates, on=category_column, how='left')
    return df

def add_holiday_feature(df, date_column, holidays):
    if date_column in df.columns:
        logger.info('Adding holiday feature...')
        df['is_holiday'] = pd.to_datetime(df[date_column]).isin(holidays)
        df['is_weekend'] = pd.to_datetime(df[date_column]).dt.weekday >= 5
    return df

def add_geospatial_features(df, postal_code_column, demographics):
    if postal_code_column in df.columns:
        logger.info('Adding geospatial features...')
        df[postal_code_column] = df[postal_code_column].astype(str)
        demographics[postal_code_column] = demographics[postal_code_column].astype(str)
        df = df.merge(demographics, on=postal_code_column, how='left')
    return df

def main():
    # Load all datasets
    datasets = {
        'plaid_transactions': load_data('plaid_transactions.xlsx'),
        'plaid_accounts': load_data('plaid_accounts.xlsx'),
        'plaid_liabilities_credit': load_data('plaid_liabilities_credit.xlsx'),
        'plaid_liabilities_credit_apr': load_data('plaid_liabilities_credit_apr.xlsx'),
        'plaid_transaction_counterparties': load_data('plaid_transaction_counterparties.xlsx'),
        'asset_report': load_data('asset_report.xlsx'),
        'asset_item': load_data('asset_item.xlsx'),
        'asset_account': load_data('asset_account.xlsx'),
        'asset_transaction': load_data('asset_transaction.xlsx'),
        'asset_historical_balance': load_data('asset_historical_balance.xlsx'),
        'mbna_accounts': load_data('mbna_accounts.xlsx'),
        'mbna_transactions': load_data('mbna_transactions.xlsx')
    }

    # Example holiday data and demographics data
    holidays = pd.to_datetime(['2024-01-01', '2024-12-25'])
    demographics = pd.DataFrame({
        'location_postal_code': ['94105', '10001'],
        'region_demographics': ['Demographics_1', 'Demographics_2']
    })

    # Apply feature engineering and enrichment to each dataset
    for name, df in datasets.items():
        if 'transactions' in name:
            df = add_temporal_features(df, 'date')
            df = add_recency_features(df, 'date', 'account_id')
            df = add_behavioral_features(df, 'account_id', 'merchant_name', 'category', 'amount')
            df = add_category_features(df, 'category', 'amount')
            df = add_holiday_feature(df, 'date', holidays)
            df = add_geospatial_features(df, 'location_postal_code', demographics)
        
        if 'accounts' in name:
            df = add_account_level_aggregates(df, 'account_id', 'current_balance')
            df = add_monthly_aggregates(df, 'date', 'account_id', 'current_balance')

        save_data(df, f'{name}_enriched.xlsx')

if __name__ == '__main__':
    main()
