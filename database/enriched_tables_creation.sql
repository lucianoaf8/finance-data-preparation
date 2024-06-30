# TRUNCATE TABLE plaid_transactions_enriched
# TRUNCATE TABLE plaid_transaction_counterparties_enriched
# TRUNCATE TABLE asset_historical_balance_enriched
# TRUNCATE TABLE asset_item_enriched
# TRUNCATE TABLE asset_report_enriched
# TRUNCATE TABLE asset_transaction_enriched
# TRUNCATE TABLE plaid_accounts_enriched
# TRUNCATE TABLE plaid_liabilities_credit_apr_enriched
# TRUNCATE TABLE plaid_liabilities_credit_enriched
# TRUNCATE TABLE mbna_accounts_enriched
# TRUNCATE TABLE mbna_transactions_enriched

# DROP TABLE plaid_transactions_enriched
CREATE TABLE plaid_transactions_enriched (
    id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for the transaction
    account_id VARCHAR(255) NOT NULL, -- Identifier for the associated account
    transaction_id VARCHAR(255) NOT NULL UNIQUE, -- Unique identifier for the transaction
    account_owner VARCHAR(255), -- Owner of the account
    amount DECIMAL(10, 2), -- Transaction amount
    authorized_date DATE, -- Date the transaction was authorized
    authorized_datetime DATETIME, -- DateTime the transaction was authorized
    date DATE, -- Date of the transaction
    datetime DATETIME, -- DateTime of the transaction
    iso_currency_code VARCHAR(10), -- ISO currency code (e.g., 'USD')
    logo_url VARCHAR(255), -- URL of the merchant's logo
    merchant_entity_id VARCHAR(255), -- Identifier for the merchant entity
    merchant_name VARCHAR(255), -- Name of the merchant
    name VARCHAR(255), -- Name of the transaction
    payment_channel VARCHAR(50), -- Payment channel (e.g., 'online')
    pending BOOLEAN, -- Whether the transaction is pending
    pending_transaction_id VARCHAR(255), -- Identifier for the pending transaction
    transaction_code VARCHAR(255), -- Transaction code
    transaction_type VARCHAR(50), -- Type of transaction (e.g., 'debit')
    unofficial_currency_code VARCHAR(10), -- Unofficial currency code, if any
    category VARCHAR(255), -- Transaction category
    category_id VARCHAR(255), -- Identifier for the transaction category
    personal_finance_category_confidence_level VARCHAR(50), -- Confidence level of the personal finance category
    personal_finance_category_detailed VARCHAR(255), -- Detailed personal finance category
    personal_finance_category_primary VARCHAR(255), -- Primary personal finance category
    personal_finance_category_icon_url VARCHAR(255), -- URL of the personal finance category icon
    location_address VARCHAR(255), -- Address of the transaction location
    location_city VARCHAR(255), -- City of the transaction location
    location_region VARCHAR(50), -- Region/State of the transaction location
    location_postal_code VARCHAR(20), -- Postal code of the transaction location
    location_country VARCHAR(50), -- Country of the transaction location
    location_lat DECIMAL(10, 7), -- Latitude of the transaction location
    location_lon DECIMAL(10, 7), -- Longitude of the transaction location
    location_store_number VARCHAR(50), -- Store number of the transaction location
    payment_meta_reference_number VARCHAR(255), -- Reference number of the payment
    payment_meta_ppd_id VARCHAR(255), -- PPD ID of the payment
    payment_meta_payee VARCHAR(255), -- Payee of the payment
    payment_meta_by_order_of VARCHAR(255), -- By order of the payment
    payment_meta_payer VARCHAR(255), -- Payer of the payment
    payment_meta_payment_method VARCHAR(255), -- Payment method
    payment_meta_payment_processor VARCHAR(255), -- Payment processor
    payment_meta_reason VARCHAR(255), -- Reason for the payment
    website VARCHAR(255), -- Website of the merchant
    check_number VARCHAR(255), -- Check number, if applicable
    file_import_id INT, -- Identifier for the associated file import
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the record was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Timestamp when the record was last updated
    day_of_week VARCHAR(50), -- Day of the week of the transaction
    transaction_hour INT, -- Hour of the transaction
    transaction_day_of_week VARCHAR(50), -- Day of the week of the transaction
    transaction_week_of_year INT, -- Week of the year of the transaction
    transaction_month INT, -- Month of the transaction
    transaction_quarter INT, -- Quarter of the transaction
    days_since_last_transaction INT, -- Days since the last transaction
    total_spent_per_category DECIMAL(10, 2), -- Total spent per category
    average_spent_per_category DECIMAL(10, 2), -- Average spent per category
    transaction_count_per_category INT -- Transaction count per category
);

CREATE TABLE plaid_transaction_counterparties_enriched (
    id INT PRIMARY KEY,
    transaction_id VARCHAR(255),
    name VARCHAR(255),
    type VARCHAR(255),
    website VARCHAR(255),
    logo_url VARCHAR(255),
    confidence_level VARCHAR(255),
    entity_id VARCHAR(255),
    phone_number VARCHAR(255),
    file_import_id INT
);

CREATE TABLE asset_account_enriched (
    account_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    official_name VARCHAR(255),
    mask INT,
    available DECIMAL(10, 2),
    current DECIMAL(10, 2),
    `limit` INT,
    margin_loan_amount DECIMAL(10, 2),
    iso_currency_code VARCHAR(10),
    unofficial_currency_code VARCHAR(10),
    type VARCHAR(255),
    subtype VARCHAR(255),
    days_available INT,
    item_id VARCHAR(255),
    asset_report_id VARCHAR(255)
);

CREATE TABLE asset_historical_balance_enriched (
    balance_id INT PRIMARY KEY,
    account_id VARCHAR(255),
    date DATE,
    current DECIMAL(10, 2),
    iso_currency_code VARCHAR(10),
    unofficial_currency_code VARCHAR(10),
    asset_report_id VARCHAR(255)
);

CREATE TABLE asset_item_enriched (
    item_id VARCHAR(255) PRIMARY KEY,
    institution_name VARCHAR(255),
    institution_id VARCHAR(255),
    date_last_updated DATETIME,
    asset_report_id VARCHAR(255)
);

CREATE TABLE asset_report_enriched (
    asset_report_id VARCHAR(255) PRIMARY KEY,
    client_report_id INT,
    date_generated DATETIME,
    days_requested INT,
    file_path VARCHAR(255),
    json_file TEXT,
    created_at DATETIME
);

CREATE TABLE asset_transaction_enriched (
    transaction_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255),
    unofficial_currency_code VARCHAR(10),
    original_description VARCHAR(255),
    date DATE,
    pending BOOLEAN,
    asset_report_id VARCHAR(255)
);

CREATE TABLE plaid_accounts_enriched (
    account_id VARCHAR(255) PRIMARY KEY,
    bank_name VARCHAR(255),
    available_balance DECIMAL(10, 2),
    current_balance DECIMAL(10, 2),
    balance_limit INT,
    iso_currency_code VARCHAR(10),
    unofficial_currency_code VARCHAR(10),
    mask VARCHAR(255),
    name VARCHAR(255),
    official_name VARCHAR(255),
    type VARCHAR(255),
    subtype VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME,
    total_transactions INT,
    total_amount_spent DECIMAL(10, 2),
    average_transaction_amount DECIMAL(10, 2),
    max_transaction_amount DECIMAL(10, 2)
);

CREATE TABLE plaid_liabilities_credit_apr_enriched (
    id INT PRIMARY KEY,
    account_id VARCHAR(255),
    apr_percentage DECIMAL(5, 2),
    apr_type VARCHAR(255),
    balance_subject_to_apr INT,
    interest_charge_amount INT,
    file_import_id INT
);

CREATE TABLE plaid_liabilities_credit_enriched (
    id INT PRIMARY KEY,
    account_id VARCHAR(255),
    is_overdue BOOLEAN,
    last_payment_amount DECIMAL(10, 2),
    last_payment_date DATE,
    last_statement_issue_date DATE,
    last_statement_balance DECIMAL(10, 2),
    minimum_payment_amount DECIMAL(10, 2),
    next_payment_due_date DATE,
    file_import_id INT
);

CREATE TABLE mbna_accounts_enriched (
    id INT PRIMARY KEY,
    cardholder_name VARCHAR(255),
    account_number VARCHAR(255),
    credit_limit DECIMAL(10, 2),
    cash_advance_limit DECIMAL(10, 2),
    credit_available DECIMAL(10, 2),
    cash_advance_available DECIMAL(10, 2),
    statement_closing_date DATE,
    annual_interest_rate_purchases DECIMAL(5, 2),
    annual_interest_rate_balance_transfers DECIMAL(5, 2),
    annual_interest_rate_cash_advances DECIMAL(5, 2)
);

# DROP TABLE mbna_transactions_enriched
CREATE TABLE mbna_transactions_enriched (
    transaction_id INT PRIMARY KEY,
    file_id INT,
    account_id VARCHAR(255),
    posting_date DATE,
    payee VARCHAR(255),
    address VARCHAR(255),
    amount DECIMAL(10, 2)
);
