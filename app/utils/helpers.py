
import pandas as pd
from ..models.stats import SimpleStat, GraphData, GraphPoints, TableData
import pandas as pd

def get_average_transaction_over_time(df: pd.DataFrame, granularity: str, filters: dict) -> GraphData:
    group_cols, label_fmt = get_grouping_and_label_fn(granularity)

    grouped = df.groupby(group_cols)["amount"].mean().reset_index()
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)

    metric_label = f"{granularity.capitalize()} Average Transaction Value{get_filter_suffix(filters)}"

    return GraphData(
        metric=metric_label,
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].round(2).tolist()
        )
    )
    
def get_days_between_transactions(df: pd.DataFrame, filters: dict) -> TableData:
    df = df.sort_values(by=["merchant_id", "customer_id", "date"])
    df["days_since"] = df.groupby(["merchant_id", "customer_id"])["date"].diff().dt.days

    return TableData(
        metric=f"Days Between Transactions per Customer{get_filter_suffix(filters)}",
        data=df[["merchant_id", "customer_id", "date", "days_since"]].to_dict(orient="records")
    )


def get_transaction_outliers(df: pd.DataFrame, filters: dict) -> TableData:
    grouped = (
        df.groupby(["merchant_id", "customer_id"])["amount"].sum()
        .reset_index()
        .sort_values(by="amount", ascending=False)
    )

    mean_amount = grouped["amount"].mean()
    std_amount = grouped["amount"].std()

    grouped["outlier"] = (
        (grouped["amount"] > (mean_amount + std_amount)) |
        (grouped["amount"] < (mean_amount - std_amount))
    )

    outliers = grouped[grouped["outlier"]]

    return TableData(
        metric=f"Customer Transaction Outliers (±1 STD){get_filter_suffix(filters)}",
        data=outliers.to_dict(orient="records")
    )


def get_customer_segmentation(df: pd.DataFrame, filters: dict) -> TableData:
    customer_spend = (
        df.groupby("customer_id")["amount"].sum().reset_index()
        .sort_values(by="amount", ascending=False)
    )

    high_value = customer_spend[customer_spend["amount"] > 800]
    mid_value = customer_spend[(customer_spend["amount"] <= 800) & (customer_spend["amount"] > 500)]
    low_value = customer_spend[customer_spend["amount"] <= 500]

    metric_label = f"Customer Segmentation by Total Spend{get_filter_suffix(filters)}"

    return TableData(
        metric=metric_label,
        data={
            "high_value": high_value.to_dict(orient="records"),
            "mid_value": mid_value.to_dict(orient="records"),
            "low_value": low_value.to_dict(orient="records"),
        }
    )

def get_top_customers(df: pd.DataFrame, mode: str, limit: int, filters: dict) -> TableData:
    if mode == "amount":
        grouped = (
            df.groupby(["merchant_id", "customer_id"])["amount"].sum()
            .reset_index()
            .sort_values(by=["merchant_id", "amount"], ascending=[True, False])
            .groupby("merchant_id")
            .head(limit)
        )
        base_metric = f"Top {limit} Customers by Amount"
    else:  # mode == "count"
        grouped = (
            df.groupby(["merchant_id", "customer_id"])["amount"].count()
            .reset_index()
            .rename(columns={"amount": "transaction_count"})
            .sort_values(by=["merchant_id", "transaction_count"], ascending=[True, False])
            .groupby("merchant_id")
            .head(limit)
        )
        base_metric = f"Top {limit} Customers by Transaction Count"

    suffix = get_filter_suffix(filters)

    return TableData(
        metric=base_metric + suffix,
        data=grouped.to_dict(orient="records")
    )


def get_transaction_volume_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    group_cols, label_fmt = get_grouping_and_label_fn(granularity)
    grouped = df.groupby(group_cols)["amount"].sum().reset_index()
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)
    
    suffix = get_filter_suffix(filters or {})

    return GraphData(
        metric=f"{granularity.capitalize()} Transaction Count{suffix}",
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].round(2).tolist()
        )
    )

def get_transaction_count_over_time(df: pd.DataFrame, granularity: str, filters: dict = None) -> GraphData:
    group_cols, label_fmt = get_grouping_and_label_fn(granularity)
    grouped = df.groupby(group_cols)["amount"].count().reset_index()
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)
    suffix = get_filter_suffix(filters or {})

    return GraphData(
        metric=f"{granularity.capitalize()} Transaction Count{suffix}",
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].tolist()
        )
    )

def get_grouping_and_label_fn(granularity: str):
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

def apply_merchant_date_filters(df, year=None, month=None, week=None, day=None, range_days=None, start_date=None, end_date=None):
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["week"] = df["date"].dt.isocalendar().week

    # Date range filters
    if start_date and end_date:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df["date"] >= start) & (df["date"] <= end)]
    elif range_days:
        end = pd.Timestamp.today().normalize()
        start = end - pd.Timedelta(days=range_days)
        df = df[(df["date"] >= start) & (df["date"] <= end)]

    # Date part filters
    if year:
        df = df[df["year"] == year]
    if month:
        df = df[df["month"] == month]
    if week:
        df = df[df["week"] == week]
    if day:
        df = df[df["day"] == day]
        
    # # Apply filters only when they make sense
    # if granularity in ("daily", "weekly", "monthly") and year:
    #     df = df[df["year"] == year]

    # if granularity in ("daily", "monthly") and month:
    #     df = df[df["month"] == month]

    # if granularity in ("daily", "weekly") and week:
    #     df = df[df["week"] == week]

    # if granularity == "daily" and day:
    #     df = df[df["day"] == day]

    return df

def get_filter_suffix(filters: dict) -> str:
    parts = []
    if filters.get("year"):
        parts.append(str(filters["year"]))
    if filters.get("month"):
        parts.append(f"Month {filters['month']}")
    if filters.get("week"):
        parts.append(f"Week {filters['week']}")
    if filters.get("day"):
        parts.append(f"Day {filters['day']}")
    if filters.get("range_days"):
        parts.append(f"Last {filters['range_days']} Days")
    if filters.get("start_date") and filters.get("end_date"):
        parts.append(f"{filters['start_date']} to {filters['end_date']}")

    return " for " + ", ".join(parts) if parts else ""
