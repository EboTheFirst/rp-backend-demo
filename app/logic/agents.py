
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
