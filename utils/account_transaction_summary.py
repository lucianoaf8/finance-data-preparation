# utils/account_transaction_summary.py
import pandas as pd
import numpy as np

def load_data(file_path):
    """
    Load the data from the specified Excel file.
    
    Args:
    file_path (str): The path to the Excel file.
    
    Returns:
    pd.DataFrame: Loaded data as a pandas DataFrame.
    """
    sheets = pd.ExcelFile(file_path).sheet_names
    print(f"Available sheets: {sheets}")
    df = pd.read_excel(file_path, sheet_name=sheets[0])
    return df

def generate_report(df):
    """
    Generate the required report from the DataFrame.
    
    Args:
    df (pd.DataFrame): The data to analyze.
    
    Returns:
    pd.DataFrame: The summary report.
    """
    # Convert transaction_date to datetime
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    
    # Create year_month for monthly analysis
    df['year_month'] = df['transaction_date'].dt.to_period('M')
    
    # Group by bank_name, account_name, and account_type
    group = df.groupby(['bank_name', 'account_name', 'account_type'])
    
    # Initialize a list to collect results
    results = []
    
    for name, data in group:
        bank_name, account_name, account_type = name
        number_of_transactions = data['transaction_id'].nunique()
        min_transaction_date = data['transaction_date'].min()
        max_transaction_date = data['transaction_date'].max()
        avg_number_of_transactions = number_of_transactions / len(data['year_month'].unique())
        min_transaction_amount = data['transaction_amount'].min()
        max_transaction_amount = data['transaction_amount'].max()
        avg_transaction_amount = data['transaction_amount'].mean()
        monthly_group = data.groupby('year_month')
        avg_monthly_number_of_transactions = monthly_group['transaction_id'].count().mean()
        avg_monthly_transaction_amount = monthly_group['transaction_amount'].mean().mean()
        
        results.append([
            bank_name, account_name, account_type, number_of_transactions, min_transaction_date,
            max_transaction_date, avg_number_of_transactions, avg_monthly_number_of_transactions,
            min_transaction_amount, max_transaction_amount, avg_transaction_amount, avg_monthly_transaction_amount
        ])
    
    columns = [
        'bank_name', 'account_name', 'account_type', 'number of transactions', 'min transaction date', 
        'max transaction date', 'avg number of transactions', 'avg monthly number of transactions', 
        'min transaction amount', 'max transaction amount', 'avg transaction amount', 'avg monthly transaction amount'
    ]
    
    report_df = pd.DataFrame(results, columns=columns)
    
    return report_df

def save_report(report_df, output_file):
    """
    Save the report to an Excel file.
    
    Args:
    report_df (pd.DataFrame): The report DataFrame.
    output_file (str): The path to the output Excel file.
    """
    report_df.to_excel(output_file, index=False)

def main():
    # Load the data
    file_path = 'data_files/base_all_accounts_transactions_Jan24-July24.xlsx'
    df = load_data(file_path)
    
    # Generate the report
    report_df = generate_report(df)
    
    # Save the report
    output_file = 'reports/account_transactions_summary_report.xlsx'
    save_report(report_df, output_file)

if __name__ == "__main__":
    main()
