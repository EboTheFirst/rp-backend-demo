
import pandas as pd
from typing import List
from app.models.stats import SimpleStat, GraphData, GraphPoints, TableData
from app.utils.analytics import (
    _get_filter_suffix, _apply_date_filters, _get_average_transaction_over_time,
    _get_days_between_transactions, _get_transaction_outliers, _get_segmentation,
    _get_top_entities, _get_transaction_volume_over_time, _get_transaction_count_over_time
)

def get_merchant_stats(df: pd.DataFrame, filters: dict) -> List[SimpleStat]:
    suffix = _get_filter_suffix(filters)

    stats = [
        SimpleStat(metric=f"Total Transaction Value{suffix}", value=round(df["amount"].sum(), 2)),
        SimpleStat(metric=f"Average Transaction Value{suffix}", value=round(df["amount"].mean(), 2)),
        SimpleStat(metric=f"Min Transaction Value{suffix}", value=round(df["amount"].min(), 2)),
        SimpleStat(metric=f"Max Transaction Value{suffix}", value=round(df["amount"].max(), 2)),
        SimpleStat(metric=f"Transaction Count{suffix}", value=int(df["amount"].count())),
        SimpleStat(metric=f"Unique Terminals{suffix}", value=int(df["terminal_id"].nunique())),
        SimpleStat(metric=f"Total Branches{suffix}", value=int(df["branch_admin_id"].nunique()))
    ]

    return stats

def get_average_transaction_over_time(df: pd.DataFrame, granularity: str, filters: dict) -> GraphData:
    return _get_average_transaction_over_time(df, granularity, filters)
    
def get_days_between_transactions(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_days_between_transactions(df, filters, entity_id_col="merchant_id")

def get_transaction_outliers(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_transaction_outliers(df, filters, entity_id_col="merchant_id")

def get_customer_segmentation(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_segmentation(
        df, 
        filters, 
        id_col="customer_id", 
        metric_prefix="Customer Segmentation by Total Spend"
    )

def get_top_customers(df: pd.DataFrame, mode: str, limit: int, filters: dict) -> TableData:
    return _get_top_entities(
        df, 
        mode, 
        limit, 
        filters, 
        entity_id_col="merchant_id", 
        target_id_col="customer_id",
        metric_prefix="Top"
    )

def get_transaction_volume_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    return _get_transaction_volume_over_time(df, granularity, filters)

def get_transaction_count_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    return _get_transaction_count_over_time(df, granularity, filters)

def apply_merchant_date_filters(df, year=None, month=None, week=None, day=None, range_days=None, start_date=None, end_date=None):
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

def get_transaction_frequency_analysis(df: pd.DataFrame, filters: dict) -> TableData:
    """Analyze transaction frequency patterns for a merchant."""
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
    
    # Prepare result
    result = {
        "summary": [
            {"metric": f"Average Daily Transactions{suffix}", "value": round(avg_daily, 2)},
            {"metric": f"Days with Activity{suffix}", "value": days_with_transactions},
            {"metric": f"Total Days in Period{suffix}", "value": total_days},
            {"metric": f"Activity Rate (%){suffix}", "value": activity_rate}
        ],
        "day_of_week": day_counts.to_dict(orient="records"),
        "hour_of_day": hour_counts.to_dict(orient="records"),
        "month_of_year": month_counts.to_dict(orient="records"),
        "quarter_of_year": quarter_counts.to_dict(orient="records")
    }
    
    return TableData(
        metric=f"Transaction Frequency Analysis{suffix}",
        data=result
    )
