
import pandas as pd
from app.models.stats import SimpleStat, GraphData, GraphPoints, TableData
from app.utils.analytics import (
    _get_filter_suffix, _apply_date_filters, _get_average_transaction_over_time,
    _get_days_between_transactions, _get_transaction_outliers, _get_segmentation,
    _get_top_entities, _get_transaction_volume_over_time, _get_transaction_count_over_time
)

def get_average_transaction_over_time(df: pd.DataFrame, granularity: str, filters: dict) -> GraphData:
    return _get_average_transaction_over_time(df, granularity, filters)
    
def get_days_between_transactions(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_days_between_transactions(df, filters, entity_id_col="terminal_id")

def get_transaction_outliers(df: pd.DataFrame, filters: dict) -> TableData:
    return _get_transaction_outliers(df, filters, entity_id_col="terminal_id")

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
        entity_id_col="terminal_id", 
        target_id_col="customer_id",
        metric_prefix="Top"
    )

def get_transaction_volume_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    return _get_transaction_volume_over_time(df, granularity, filters)

def get_transaction_count_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    return _get_transaction_count_over_time(df, granularity, filters)

def apply_terminal_date_filters(df, year=None, month=None, week=None, day=None, range_days=None, start_date=None, end_date=None):
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
