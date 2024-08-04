# scripts/anomaly_detection.py
import numpy as np
from scipy import stats
import logging

def detect_anomalies(df):
    """
    Implement basic anomaly detection and flag potential fraudulent activities.
    """
    try:
        if 'transaction_amount' not in df.columns:
            logging.warning("'transaction_amount' column not found. Skipping anomaly detection.")
            return df

        # Z-score method for transaction amounts
        z_scores = np.abs(stats.zscore(df['transaction_amount']))
        df['is_amount_anomaly'] = z_scores > 3
        
        # Flag sudden large transactions
        df['is_large_transaction'] = (df['transaction_amount'].abs() > df.groupby('account_id')['transaction_amount'].transform('mean') * 5)
        
        # Flag high frequency of transactions
        transaction_frequency = df.groupby('account_id')['transaction_id'].transform('count')
        df['is_high_frequency'] = transaction_frequency > transaction_frequency.quantile(0.95)
        
        # Combine flags
        df['potential_fraud'] = (df['is_amount_anomaly'] | df['is_large_transaction'] | df['is_high_frequency']).astype(int)
        
        logging.info("Anomaly detection completed successfully")
        return df
    except Exception as e:
        logging.error(f"Error in anomaly detection: {str(e)}")
        raise