from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN

def isolation_forest_anomalies(df):
    clf = IsolationForest(contamination=0.1, random_state=42)
    df['is_anomaly_isolation_forest'] = clf.fit_predict(df[['transaction_amount', '7day_avg', '30day_avg']])
    return df

def dbscan_anomalies(df):
    dbscan = DBSCAN(eps=0.5, min_samples=5)
    df['is_anomaly_dbscan'] = dbscan.fit_predict(df[['transaction_amount', '7day_avg', '30day_avg']])
    return df

# Call these functions in the main pipeline