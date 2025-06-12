import pandas as pd
from typing import List
from ..models.stats import SimpleStat, GraphData, GraphPoints, TableData
from ..core.analytics_config import (
    CUSTOMER_SEGMENTATION, MERCHANT_SEGMENTATION, 
    OUTLIER_DETECTION, TIME_FORMATS
)

def _get_filter_suffix(filters: dict) -> str:
    """Generate a suffix string based on applied filters."""
    if not filters:
        return ""
    
    parts = []
    for key, value in filters.items():
        if value is not None:
            parts.append(f"{key}={value}")
    
    if not parts:
        return ""
    
    return f" ({', '.join(parts)})"

def _get_grouping_and_label_fn(granularity: str):
    """Return grouping columns and label formatting function based on time granularity."""
    if granularity == "daily":
        return (
            ["year", "month", "day"],
            lambda r: f"{int(r['year']):04d}-{int(r['month']):02d}-{int(r['day']):02d}"
        )
    elif granularity == "weekly":
        return (
            ["year", "week"],
            lambda r: f"{int(r['year']):04d}-W{int(r['week']):02d}"
        )
    elif granularity == "monthly":
        return (
            ["year", "month"],
            lambda r: f"{int(r['year']):04d}-{int(r['month']):02d}"
        )
    else:  # yearly
        return (
            ["year"],
            lambda r: f"{int(r['year']):04d}"
        )

def _prepare_date_columns(df):
    """Prepare date-related columns for analysis."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["week"] = df["date"].dt.isocalendar().week
    return df

def _apply_date_filters(df, year=None, month=None, week=None, day=None, 
                      range_days=None, start_date=None, end_date=None):
    """Apply date filters to a dataframe."""
    df = _prepare_date_columns(df)
    
    # Date range filters
    if start_date and end_date:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df["date"] >= start) & (df["date"] <= end)]
    elif range_days:
        end = pd.Timestamp.today().normalize()
        start = end - pd.Timedelta(days=range_days)
        df = df[(df["date"] >= start) & (df["date"] <= end)]
    
    # Individual date component filters
    if year is not None:
        df = df[df["year"] == year]
    if month is not None:
        df = df[df["month"] == month]
    if week is not None:
        df = df[df["week"] == week]
    if day is not None:
        df = df[df["day"] == day]
        
    return df

def _get_average_transaction_over_time(df: pd.DataFrame, granularity: str, 
                                     filters: dict, entity_type: str = None) -> GraphData:
    """Calculate average transaction amount over time."""
    group_cols, label_fmt = _get_grouping_and_label_fn(granularity)

    grouped = df.groupby(group_cols)["amount"].mean().reset_index()
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)

    metric_label = f"{granularity.capitalize()} Average Transaction Value{_get_filter_suffix(filters)}"

    return GraphData(
        metric=metric_label,
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].round(2).tolist()
        )
    )

def _get_days_between_transactions(df: pd.DataFrame, filters: dict, 
                                 entity_id_col: str, customer_id_col: str = "customer_id") -> TableData:
    """Calculate days between transactions for each customer."""
    df = df.sort_values(by=[entity_id_col, customer_id_col, "date"])
    df["days_since"] = df.groupby([entity_id_col, customer_id_col])["date"].diff().dt.days

    return TableData(
        metric=f"Days Between Transactions per Customer{_get_filter_suffix(filters)}",
        data=df[[entity_id_col, customer_id_col, "date", "days_since"]].to_dict(orient="records")
    )

def _get_transaction_outliers(df: pd.DataFrame, filters: dict, 
                            entity_id_col: str, target_id_col: str = "customer_id") -> TableData:
    """Identify transaction outliers based on standard deviation."""
    grouped = (
        df.groupby([entity_id_col, target_id_col])["amount"].sum()
        .reset_index()
        .sort_values(by="amount", ascending=False)
    )

    mean_amount = grouped["amount"].mean()
    std_amount = grouped["amount"].std()
    std_multiplier = OUTLIER_DETECTION["std_multiplier"]

    grouped["outlier"] = (
        (grouped["amount"] > (mean_amount + std_amount * std_multiplier)) |
        (grouped["amount"] < (mean_amount - std_amount * std_multiplier))
    )

    outliers = grouped[grouped["outlier"]]
    
    target_type = "Customer" if target_id_col == "customer_id" else "Merchant"

    return TableData(
        metric=f"{target_type} Transaction Outliers (±{std_multiplier} STD){_get_filter_suffix(filters)}",
        data=outliers.to_dict(orient="records")
    )

def _get_segmentation(df: pd.DataFrame, filters: dict, 
                    id_col: str = "customer_id", 
                    metric_prefix: str = "Customer Segmentation") -> TableData:
    """Segment entities based on total amount."""
    entity_total = (
        df.groupby(id_col)["amount"].sum().reset_index()
        .sort_values(by="amount", ascending=False)
    )

    # Use appropriate thresholds based on entity type
    if id_col == "customer_id":
        high_threshold = CUSTOMER_SEGMENTATION["high_threshold"]
        mid_threshold = CUSTOMER_SEGMENTATION["mid_threshold"]
    elif id_col == "merchant_id":
        high_threshold = MERCHANT_SEGMENTATION["high_threshold"]
        mid_threshold = MERCHANT_SEGMENTATION["mid_threshold"]
    else:
        # Default thresholds for other entity types
        high_threshold = 800
        mid_threshold = 500

    high_value = entity_total[entity_total["amount"] > high_threshold]
    mid_value = entity_total[(entity_total["amount"] <= high_threshold) & 
                            (entity_total["amount"] > mid_threshold)]
    low_value = entity_total[entity_total["amount"] <= mid_threshold]

    metric_label = f"{metric_prefix}{_get_filter_suffix(filters)}"

    return TableData(
        metric=metric_label,
        data={
            "high_value": high_value.to_dict(orient="records"),
            "mid_value": mid_value.to_dict(orient="records"),
            "low_value": low_value.to_dict(orient="records"),
        }
    )

def _get_top_entities(df: pd.DataFrame, mode: str, limit: int, filters: dict,
                    entity_id_col: str, target_id_col: str,
                    metric_prefix: str = "Top") -> TableData:
    """Get top entities by amount or count, but always include both metrics."""

    # Calculate both amount and count for each entity
    grouped_stats = (
        df.groupby([entity_id_col, target_id_col])
        .agg({
            'amount': ['sum', 'count']
        })
        .reset_index()
    )

    # Flatten column names
    grouped_stats.columns = [entity_id_col, target_id_col, 'total_amount', 'transaction_count']

    # Add entity name if available
    name_col = target_id_col.replace('_id', '_name')
    if name_col in df.columns:
        # Get the name for each entity (take first occurrence)
        entity_names = df.groupby(target_id_col)[name_col].first().reset_index()
        grouped_stats = grouped_stats.merge(entity_names, on=target_id_col, how='left')

    # Sort by the specified mode and take top N
    if mode == "amount":
        sorted_data = (
            grouped_stats
            .sort_values(by=[entity_id_col, "total_amount"], ascending=[True, False])
            .groupby(entity_id_col)
            .head(limit)
        )
        base_metric = f"{metric_prefix} {limit} {target_id_col.replace('_id', '').title()}s by Amount"
    else:  # mode == "count"
        sorted_data = (
            grouped_stats
            .sort_values(by=[entity_id_col, "transaction_count"], ascending=[True, False])
            .groupby(entity_id_col)
            .head(limit)
        )
        base_metric = f"{metric_prefix} {limit} {target_id_col.replace('_id', '').title()}s by Transaction Count"

    suffix = _get_filter_suffix(filters)

    return TableData(
        metric=base_metric + suffix,
        data=sorted_data.to_dict(orient="records")
    )

def _get_transaction_volume_over_time(df: pd.DataFrame, granularity: str, 
                                    filters: dict = None) -> GraphData:
    """Calculate transaction volume over time."""
    group_cols, label_fmt = _get_grouping_and_label_fn(granularity)
    grouped = df.groupby(group_cols)["amount"].sum().reset_index()
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)
    
    suffix = _get_filter_suffix(filters or {})

    return GraphData(
        metric=f"{granularity.capitalize()} Transaction Volume{suffix}",
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].round(2).tolist()
        )
    )

def _get_transaction_count_over_time(df: pd.DataFrame, granularity: str, 
                                   filters: dict = None) -> GraphData:
    """Calculate transaction count over time."""
    group_cols, label_fmt = _get_grouping_and_label_fn(granularity)
    grouped = df.groupby(group_cols)["amount"].count().reset_index()
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)
    suffix = _get_filter_suffix(filters or {})

    return GraphData(
        metric=f"{granularity.capitalize()} Transaction Count{suffix}",
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].tolist()
        )
    )

def _get_transaction_metrics_per_entity(df: pd.DataFrame, granularity: str, 
                                      filters: dict = None, 
                                      entity_id_col: str = "merchant_id",
                                      metric_type: str = "volume") -> dict:
    """Calculate transaction metrics per entity over time."""
    group_cols, label_fmt = _get_grouping_and_label_fn(granularity)
    group_cols = [entity_id_col] + group_cols

    if metric_type == "volume":
        grouped = df.groupby(group_cols)["amount"].sum().reset_index()
        metric_name = "Transaction Volume"
    else:  # count
        grouped = df.groupby(group_cols)["amount"].count().reset_index()
        metric_name = "Transaction Count"
        
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)

    suffix = _get_filter_suffix(filters or {})

    result = {}
    for entity_id, entity_df in grouped.groupby(entity_id_col):
        result[str(entity_id)] = GraphData(
            metric=f"{granularity.capitalize()} {metric_name}{suffix}",
            data=GraphPoints(
                labels=entity_df["label"].tolist(),
                values=entity_df["amount"].round(2).tolist()
            )
        )

    return result

def _safe_process_dataframe(df: pd.DataFrame, process_fn, default_result=None):
    """
    Safely process a DataFrame, handling empty DataFrames gracefully.
    
    Args:
        df: The DataFrame to process
        process_fn: Function that processes the DataFrame
        default_result: Value to return if DataFrame is empty
        
    Returns:
        The result of process_fn or default_result if df is empty
    """
    if df.empty:
        return default_result
    try:
        return process_fn(df)
    except Exception as e:
        # Log the error here if you have logging configured
        print(f"Error processing DataFrame: {e}")
        return default_result
