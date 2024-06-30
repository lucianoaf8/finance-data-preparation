### **Data Cleaning (data_cleaning.py)**

The `data_cleaning.py` script performs various data cleaning operations on different tables. Below are the details of the cleaning steps:

1. **General Cleaning Functions**:
    - **save_cleaned_data(df, table_name)**: Saves the cleaned dataframe to an Excel file.
    - **remove_duplicates(df, unique_column)**: Removes duplicate rows based on a specified unique column.
    - **handle_missing_values(df, column, default_value)**: Replaces missing values in a specified column with a default value.
    - **standardize_addresses(df, address_columns)**: Standardizes address columns by stripping whitespace and converting to title case.
    - **format_dates(df, date_columns)**: Formats specified date columns to 'YYYY-MM-DD' format and replaces invalid dates with NaN.
    - **ensure_data_types(df, column_types)**: Ensures specified columns have the desired data types.
    - **check_referential_integrity(df, foreign_keys)**: Ensures that foreign key values exist in the reference dataframe.
    - **add_derived_columns(df, derived_columns)**: Adds new columns based on specified logic.
2. **Table-Specific Cleaning Steps**:
    - **plaid_accounts**:
        - Removes duplicates based on 'account_id'.
        - Handles missing values for 'balance_limit' and 'iso_currency_code'.
    - **plaid_liabilities_credit**:
        - Removes duplicates based on 'account_id'.
        - Handles missing values for 'last_payment_amount', 'last_statement_balance', and 'minimum_payment_amount'.
    - **plaid_liabilities_credit_apr**:
        - Removes duplicates based on 'account_id'.
        - Handles missing values for 'apr_percentage', 'balance_subject_to_apr', and 'interest_charge_amount'.
    - **plaid_transactions**:
        - Removes duplicates based on 'transaction_id'.
        - Handles missing values for 'amount', 'iso_currency_code', 'location_lat', and 'location_lon'.
        - Checks referential integrity for 'account_id' against 'plaid_accounts'.
        - Adds a derived column 'day_of_week'.
    - **plaid_transaction_counterparties**:
        - Removes duplicates based on 'transaction_id'.
        - Handles missing values for 'name', 'type', and 'confidence_level'.
    - **asset_report**:
        - Removes duplicates based on 'asset_report_id'.
        - Handles missing values for 'json_file'.
    - **asset_item**:
        - Removes duplicates based on 'item_id'.
    - **asset_account**:
        - Removes duplicates based on 'account_id'.
        - Handles missing values for 'available', 'current', 'limit', and 'iso_currency_code'.
    - **asset_transaction**:
        - Removes duplicates based on 'transaction_id'.
        - Handles missing values for 'amount'.
    - **asset_historical_balance**:
        - Removes duplicates based on 'balance_id'.
        - Handles missing values for 'current'.
    - **mbna_accounts**:
        - Removes duplicates based on 'id'.
        - Handles missing values for 'credit_limit', 'cash_advance_limit', 'credit_available', 'cash_advance_available', 'annual_interest_rate_purchases', 'annual_interest_rate_balance_transfers', and 'annual_interest_rate_cash_advances'.
    - **mbna_transactions**:
        - Removes duplicates based on 'transaction_id'.
        - Handles missing values for 'amount'.

---

### **Feature Engineering (feature_engineering.py)**

The `feature_engineering.py` script adds various features to the cleaned dataframes to enrich the data for analysis. Below are the details of the feature engineering steps:

1. **General Functions**:
    - **load_data(file_name)**: Loads data from an Excel file.
    - **save_data(df, file_name)**: Saves the enriched dataframe to an Excel file.
2. **Feature Engineering Functions**:
    - **add_temporal_features(df, date_column)**:
        - Adds 'transaction_hour', 'transaction_day_of_week', 'transaction_week_of_year', 'transaction_month', and 'transaction_quarter' based on the specified date column.
    - **add_recency_features(df, date_column, id_column)**:
        - Adds 'days_since_last_transaction' based on the difference in dates grouped by the specified id column.
    - **add_account_level_aggregates(df, id_column, amount_column)**:
        - Adds 'total_transactions', 'total_amount_spent', 'average_transaction_amount', and 'max_transaction_amount' by aggregating the amount column grouped by the specified id column.
    - **add_monthly_aggregates(df, date_column, id_column, amount_column)**:
        - Adds monthly aggregates such as 'monthly_total_spent', 'monthly_transaction_count', and 'monthly_average_transaction_amount' by grouping by id and date columns.
    - **add_behavioral_features(df, id_column, merchant_column, category_column, amount_column)**:
        - Adds 'most_frequent_merchant', 'most_frequent_category', and 'spending_variance' based on the specified columns.
    - **add_category_features(df, category_column, amount_column)**:
        - Adds 'total_spent_per_category', 'average_spent_per_category', and 'transaction_count_per_category' by aggregating the amount column grouped by the specified category column.
    - **add_holiday_feature(df, date_column, holidays)**:
        - Adds 'is_holiday' and 'is_weekend' based on the specified date column.
    - **add_geospatial_features(df, postal_code_column, demographics)**:
        - Merges the dataframe with demographic data based on the specified postal code column.
3. **Example Application**:
    - Loads various datasets such as 'plaid_transactions', 'plaid_accounts', 'plaid_liabilities_credit', 'plaid_transaction_counterparties', 'asset_report', 'asset_item', 'asset_account', 'asset_transaction', 'asset_historical_balance', 'mbna_accounts', and 'mbna_transactions'.
    - Applies the appropriate feature engineering functions to each dataset.
    - Saves the enriched data to separate Excel files.