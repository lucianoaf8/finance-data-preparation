from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

def train_category_predictor(df):
    X = df[['transaction_amount', '7day_avg', '30day_avg', 'day_of_week', 'day_of_month']]
    y = df['personal_finance_category_primary']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred))

    return model

# Implement this in the main pipeline