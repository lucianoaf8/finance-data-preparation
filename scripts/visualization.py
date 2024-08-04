import plotly.express as px
import plotly.graph_objects as go

def create_spending_pattern_chart(df):
    fig = px.bar(df.groupby('day_of_week')['transaction_amount'].mean().reset_index(), 
                 x='day_of_week', y='transaction_amount', title='Average Spending by Day of Week')
    return fig

def create_balance_trend_chart(df):
    fig = px.line(df.groupby('transaction_date')['account_current_balance'].mean().reset_index(), 
                  x='transaction_date', y='account_current_balance', title='Account Balance Trend')
    return fig

# Create more visualization functions as needed