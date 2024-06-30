CREATE TABLE plaid_transactions_enriched (
    id INT PRIMARY KEY,
    account_id VARCHAR(255),
    transaction_id VARCHAR(255),
    account_owner VARCHAR(255),
    amount DECIMAL(10, 2),
    authorized_date DATE,
    authorized_datetime DATETIME,
    date DATE,
    datetime DATETIME,
    iso_currency_code VARCHAR(10),
    unofficial_currency_code VARCHAR(10),
    category_id VARCHAR(255),
    category VARCHAR(255),
    category_type VARCHAR(255),
    pending BOOLEAN,
    pending_transaction_id VARCHAR(255),
    transaction_code VARCHAR(255),
    payment_channel VARCHAR(255),
    transaction_type VARCHAR(255),
    merchant_name VARCHAR(255),
    check_number VARCHAR(255),
    payment_method VARCHAR(255),
    transaction_status VARCHAR(255),
    location_id INT,
    merchant_id INT,
    days_since_last_transaction INT,
    merchant_name_most_frequent VARCHAR(255),
    category_most_frequent VARCHAR(255),
    spending_variance DECIMAL(15, 10),
    total_spent_per_category DECIMAL(10, 2),
    last_payment_date DATE,
    last_statement_issue_date DATE,
    last_statement_balance DECIMAL(10, 2),
    minimum_payment_amount DECIMAL(10, 2),
    next_payment_due_date DATE,
    file_import_id INT
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

CREATE TABLE mbna_transactions_enriched (
    transaction_id INT PRIMARY KEY,
    file_id INT,
    account_id VARCHAR(255),
    posting_date DATE,
    payee VARCHAR(255),
    address VARCHAR(255),
    amount DECIMAL(10, 2),
    spending_variance DECIMAL(15, 10)
);
