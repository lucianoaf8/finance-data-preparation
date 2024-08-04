from statsmodels.tsa.arima.model import ARIMA
from prophet import Prophet

def arima_forecast(df, account_id, days_to_forecast=30):
    account_data = df[df['account_id'] == account_id].set_index('transaction_date')['transaction_amount']
    model = ARIMA(account_data, order=(1,1,1))
    results = model.fit()
    forecast = results.forecast(steps=days_to_forecast)
    return forecast

def prophet_forecast(df, account_id, days_to_forecast=30):
    account_data = df[df['account_id'] == account_id][['transaction_date', 'transaction_amount']]
    account_data.columns = ['ds', 'y']
    model = Prophet()
    model.fit(account_data)
    future_dates = model.make_future_dataframe(periods=days_to_forecast)
    forecast = model.predict(future_dates)
    return forecast

# Implement these forecasts for each account in the main pipeline