
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
