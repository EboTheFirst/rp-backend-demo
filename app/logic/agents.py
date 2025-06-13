
import pandas as pd
import numpy as np
from app.models.stats import SimpleStat, GraphData, GraphPoints, TableData
from app.utils.analytics import (
    _get_filter_suffix, _apply_date_filters, _get_average_transaction_over_time,
    _get_days_between_transactions, _get_transaction_outliers, _get_segmentation,
    _get_top_entities, _get_transaction_volume_over_time, _get_transaction_count_over_time,
    _get_transaction_metrics_per_entity
)

def get_average_transaction_over_time(df: pd.DataFrame, granularity: str, filters: dict) -> GraphData:
    return _get_average_transaction_over_time(df, granularity, filters)
    
def get_days_between_transactions(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_days_between_transactions(df, filters, entity_id_col="agent_id")

def get_transaction_outliers(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_transaction_outliers(df, filters, entity_id_col="agent_id")

def get_transaction_outliers_merchants(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_transaction_outliers(
        df, 
        filters, 
        entity_id_col="agent_id", 
        target_id_col="merchant_id"
    )

def get_merchant_segmentation(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_segmentation(
        df, 
        filters, 
        id_col="merchant_id", 
        metric_prefix="Merchant Segmentation by Total Sales"
    )
    
def get_customer_segmentation(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_segmentation(
        df, 
        filters, 
        id_col="customer_id", 
        metric_prefix="Customer Segmentation by Total Spend"
    )

def get_top_merchants(df: pd.DataFrame, mode: str, limit: int, filters: dict) -> TableData:
    return _get_top_entities(
        df, 
        mode, 
        limit, 
        filters, 
        entity_id_col="agent_id", 
        target_id_col="merchant_id",
        metric_prefix="Top"
    )

def get_top_customers(df: pd.DataFrame, mode: str, limit: int, filters: dict) -> TableData:
    return _get_top_entities(
        df, 
        mode, 
        limit, 
        filters, 
        entity_id_col="agent_id", 
        target_id_col="customer_id",
        metric_prefix="Top"
    )

def get_transaction_volume_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    return _get_transaction_volume_over_time(df, granularity, filters)

def get_transaction_volume_per_merchant(df: pd.DataFrame, granularity: str, filters: dict = None) -> dict:
    return _get_transaction_metrics_per_entity(
        df, 
        granularity, 
        filters, 
        entity_id_col="merchant_id", 
        metric_type="volume"
    )

def get_transaction_count_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    return _get_transaction_count_over_time(df, granularity, filters)

def get_transaction_frequency_analysis(df: pd.DataFrame, filters: dict) -> TableData:
    """Analyze transaction frequency patterns for an agent."""
    suffix = _get_filter_suffix(filters)

    # Get transaction counts by day of week
    df['day_of_week'] = df['date'].dt.day_name()
    day_counts = df.groupby('day_of_week')['transaction_id'].count().reset_index()
    day_counts.columns = ['day_of_week', 'transaction_count']
    day_counts = day_counts.sort_values(by='transaction_count', ascending=False)

    # Get transaction counts by hour of day
    df['hour_of_day'] = df['date'].dt.hour
    hour_counts = df.groupby('hour_of_day')['transaction_id'].count().reset_index()
    hour_counts.columns = ['hour_of_day', 'transaction_count']
    hour_counts = hour_counts.sort_values(by='transaction_count', ascending=False)

    # Get transaction counts by month of year
    df['month_of_year'] = df['date'].dt.month_name()
    month_counts = df.groupby('month_of_year')['transaction_id'].count().reset_index()
    month_counts.columns = ['month_of_year', 'transaction_count']
    month_counts = month_counts.sort_values(by='transaction_count', ascending=False)

    # Get transaction counts by quarter of year
    df['quarter_of_year'] = 'Q' + df['date'].dt.quarter.astype(str)
    quarter_counts = df.groupby('quarter_of_year')['transaction_id'].count().reset_index()
    quarter_counts.columns = ['quarter_of_year', 'transaction_count']
    quarter_counts = quarter_counts.sort_values(by='transaction_count', ascending=False)

    # Calculate average transactions per day
    avg_daily = df.groupby(df['date'].dt.date)['transaction_id'].count().mean()

    # Calculate days with transactions vs total days in period
    total_days = (df['date'].max() - df['date'].min()).days + 1
    days_with_transactions = df['date'].dt.date.nunique()
    activity_rate = round((days_with_transactions / total_days) * 100, 2) if total_days > 0 else 0

    # Agent-specific metrics
    unique_merchants = df['merchant_id'].nunique()
    unique_customers = df['customer_id'].nunique()
    unique_terminals = df['terminal_id'].nunique()

    # Prepare result
    result = {
        "summary": [
            {"metric": f"Average Daily Transactions{suffix}", "value": round(avg_daily, 2)},
            {"metric": f"Days with Activity{suffix}", "value": days_with_transactions},
            {"metric": f"Total Days in Period{suffix}", "value": total_days},
            {"metric": f"Activity Rate (%){suffix}", "value": activity_rate},
            {"metric": f"Unique Merchants{suffix}", "value": unique_merchants},
            {"metric": f"Unique Customers{suffix}", "value": unique_customers},
            {"metric": f"Unique Terminals{suffix}", "value": unique_terminals}
        ],
        "day_of_week": day_counts.to_dict(orient="records"),
        "hour_of_day": hour_counts.to_dict(orient="records"),
        "month_of_year": month_counts.to_dict(orient="records"),
        "quarter_of_year": quarter_counts.to_dict(orient="records")
    }

    return TableData(
        metric=f"Agent Transaction Frequency Analysis{suffix}",
        data=result
    )

def get_transaction_count_per_merchant(df: pd.DataFrame, granularity: str, filters: dict = None) -> dict:
    return _get_transaction_metrics_per_entity(
        df, 
        granularity, 
        filters, 
        entity_id_col="merchant_id", 
        metric_type="count"
    )

def apply_agent_date_filters(df, year=None, month=None, week=None, day=None, range_days=None, start_date=None, end_date=None):
    return _apply_date_filters(
        df, 
        year=year, 
        month=month, 
        week=week, 
        day=day, 
        range_days=range_days, 
        start_date=start_date, 
        end_date=end_date
    )

def get_merchant_activity_heatmap(df: pd.DataFrame, granularity: str, filters: dict = None) -> dict:
    """
    Generate a heatmap of transaction volumes and values across merchants for an agent.
    
    Args:
        df: DataFrame containing transaction data
        granularity: Time granularity (daily, weekly, monthly, yearly)
        filters: Dictionary of filters applied to the data
        
    Returns:
        Dictionary containing heatmap data for transaction volumes and values
    """
    suffix = _get_filter_suffix(filters or {})
    
    # Set up time periods based on granularity
    if granularity == 'daily':
        df['period'] = df['date'].dt.date
    elif granularity == 'weekly':
        df['period'] = df['date'].dt.isocalendar().week
        df['period_label'] = 'Week ' + df['period'].astype(str)
    elif granularity == 'monthly':
        df['period'] = df['date'].dt.month
        df['period_label'] = df['date'].dt.month_name()
    elif granularity == 'yearly':
        df['period'] = df['date'].dt.year
        df['period_label'] = df['period'].astype(str)
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")
    
    # Get unique merchants and periods
    merchants = df['merchant_id'].unique()
    
    if granularity == 'daily':
        periods = sorted(df['period'].unique())
        period_labels = [str(p) for p in periods]
    else:
        # For other granularities, we need to maintain the order
        period_mapping = df[['period', 'period_label']].drop_duplicates()
        period_mapping = period_mapping.sort_values('period')
        periods = period_mapping['period'].tolist()
        period_labels = period_mapping['period_label'].tolist()
    
    # Initialize results
    volume_data = []
    count_data = []
    avg_value_data = []
    
    # Calculate metrics for each merchant and period
    for merchant in merchants:
        merchant_df = df[df['merchant_id'] == merchant]
        
        # Get merchant name or ID
        merchant_name = merchant  # Use ID as fallback
        if 'merchant_name' in merchant_df.columns:
            merchant_name = merchant_df['merchant_name'].iloc[0]
        
        # Calculate metrics for each period
        volume_row = {'merchant': merchant_name}
        count_row = {'merchant': merchant_name}
        avg_row = {'merchant': merchant_name}
        
        for period, label in zip(periods, period_labels):
            period_df = merchant_df[merchant_df['period'] == period]
            
            # Transaction volume (sum of amounts)
            volume = round(period_df['amount'].sum(), 2)
            volume_row[label] = volume
            
            # Transaction count
            count = len(period_df)
            count_row[label] = count
            
            # Average transaction value
            avg_value = round(period_df['amount'].mean(), 2) if count > 0 else 0
            avg_row[label] = avg_value
        
        volume_data.append(volume_row)
        count_data.append(count_row)
        avg_value_data.append(avg_row)
    
    # Sort data by total volume
    if volume_data:
        # Calculate total volume for each merchant
        for row in volume_data:
            row['total'] = sum(v for k, v in row.items() if k != 'merchant' and k != 'total')
        
        # Sort by total volume
        volume_data.sort(key=lambda x: x['total'], reverse=True)
        
        # Remove total column used for sorting
        for row in volume_data:
            del row['total']
        
        # Sort count and avg data to match volume data order
        merchant_order = [row['merchant'] for row in volume_data]
        count_data.sort(key=lambda x: merchant_order.index(x['merchant']))
        avg_value_data.sort(key=lambda x: merchant_order.index(x['merchant']))
    
    return {
        "metric": f"Merchant Activity Heatmap{suffix}",
        "periods": period_labels,
        "transaction_volume": volume_data,
        "transaction_count": count_data,
        "average_transaction_value": avg_value_data
    }
