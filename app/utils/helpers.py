
import pandas as pd
from ..models.stats import SimpleStat, GraphData, GraphPoints, TableData

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

def add_computed_attributes(df, id_col):
    # Core numeric aggregates for amount
    agg_df = df.groupby(id_col)['amount'].agg([
        ('avg_transaction_amount', 'mean'),
        ('total_transactions', 'count'),
        ('sum_transaction_amount', 'sum'),
        ('min_transaction_amount', 'min'),
        ('max_transaction_amount', 'max'),
        ('std_transaction_amount', 'std')
    ]).reset_index()

    # Always compute unique customers
    if id_col != 'customer_id':
        unique_customers = df.groupby(id_col)['customer_id'].nunique().reset_index()
        unique_customers.rename(columns={'customer_id': 'unique_customers'}, inplace=True)
        agg_df = agg_df.merge(unique_customers, on=id_col, how='left')

    # Dynamically compute other unique counts
    if id_col == 'merchant_id':
        # Unique branch admins
        unique_branch_admins = df.groupby(id_col)['branch_admin_id'].nunique().reset_index()
        unique_branch_admins.rename(columns={'branch_admin_id': 'unique_branch_admins'}, inplace=True)
        agg_df = agg_df.merge(unique_branch_admins, on=id_col, how='left')

        # Unique terminals
        unique_terminals = df.groupby(id_col)['terminal_id'].nunique().reset_index()
        unique_terminals.rename(columns={'terminal_id': 'unique_terminals'}, inplace=True)
        agg_df = agg_df.merge(unique_terminals, on=id_col, how='left')

    elif id_col == 'branch_admin_id':
        # Unique terminals
        unique_terminals = df.groupby(id_col)['terminal_id'].nunique().reset_index()
        unique_terminals.rename(columns={'terminal_id': 'unique_terminals'}, inplace=True)
        agg_df = agg_df.merge(unique_terminals, on=id_col, how='left')

    # Merge computed attributes back to the main DataFrame
    df = df.merge(agg_df, on=id_col, how='left')

    return df

# def add_computed_attributes(df, id_col):
#     # Core numeric aggregates for amount
#     agg_df = df.groupby(id_col)['amount'].agg([
#         ('avg_transaction_amount', 'mean'),
#         ('total_transactions', 'count'),
#         ('sum_transaction_amount', 'sum'),
#         ('min_transaction_amount', 'min'),
#         ('max_transaction_amount', 'max'),
#         ('std_transaction_amount', 'std')
#     ]).reset_index()

#     # Always compute unique customers
#     if id_col != 'customer_id':
#         unique_customers = df.groupby(id_col)['customer_id'].nunique().reset_index()
#         unique_customers.rename(columns={'customer_id': 'unique_customers'}, inplace=True)
#         agg_df = agg_df.merge(unique_customers, on=id_col, how='left')

#     # Dynamically compute other unique counts
#     if id_col == 'agent_id':
#         # Unique merchants
#         unique_merchants = df.groupby(id_col)['merchant_id'].nunique().reset_index()
#         unique_merchants.rename(columns={'merchant_id': 'unique_merchants'}, inplace=True)
#         agg_df = agg_df.merge(unique_merchants, on=id_col, how='left')

#         # Unique branch admins
#         unique_branch_admins = df.groupby(id_col)['branch_admin_id'].nunique().reset_index()
#         unique_branch_admins.rename(columns={'branch_admin_id': 'unique_branch_admins'}, inplace=True)
#         agg_df = agg_df.merge(unique_branch_admins, on=id_col, how='left')

#         # Unique terminals
#         unique_terminals = df.groupby(id_col)['terminal_id'].nunique().reset_index()
#         unique_terminals.rename(columns={'terminal_id': 'unique_terminals'}, inplace=True)
#         agg_df = agg_df.merge(unique_terminals, on=id_col, how='left')

#     elif id_col == 'merchant_id':
#         # Unique branch admins
#         unique_branch_admins = df.groupby(id_col)['branch_admin_id'].nunique().reset_index()
#         unique_branch_admins.rename(columns={'branch_admin_id': 'unique_branch_admins'}, inplace=True)
#         agg_df = agg_df.merge(unique_branch_admins, on=id_col, how='left')

#         # Unique terminals
#         unique_terminals = df.groupby(id_col)['terminal_id'].nunique().reset_index()
#         unique_terminals.rename(columns={'terminal_id': 'unique_terminals'}, inplace=True)
#         agg_df = agg_df.merge(unique_terminals, on=id_col, how='left')

#     elif id_col == 'branch_admin_id':
#         # Unique terminals
#         unique_terminals = df.groupby(id_col)['terminal_id'].nunique().reset_index()
#         unique_terminals.rename(columns={'terminal_id': 'unique_terminals'}, inplace=True)
#         agg_df = agg_df.merge(unique_terminals, on=id_col, how='left')

#     # Merge computed attributes back to the main DataFrame
#     df = df.merge(agg_df, on=id_col, how='left')

#     return df


import pandas as pd

def apply_filter(df, filter_obj):
    if 'and' in filter_obj:
        masks = [apply_filter(df, f) for f in filter_obj['and']]
        return pd.concat(masks, axis=1).all(axis=1)

    elif 'or' in filter_obj:
        masks = [apply_filter(df, f) for f in filter_obj['or']]
        return pd.concat(masks, axis=1).any(axis=1)

    elif 'not' in filter_obj:
        mask = apply_filter(df, filter_obj['not'])
        return ~mask

    else:
        col = filter_obj['column']
        op = filter_obj['operator']
        val = filter_obj['value']

        if col not in df.columns:
            raise ValueError(f"Unsupported column: '{col}'")

        s = df[col]
        if op == 'equals':
            return s == val
        elif op == 'not_equals':
            return s != val
        elif op == 'greater_than':
            return s > val
        elif op == 'greater_than_equals':
            return s >= val
        elif op == 'less_than':
            return s < val
        elif op == 'less_than_equals':
            return s <= val
        elif op == 'between':
            return s.between(val[0], val[1])
        elif op == 'in':
            return s.isin(val)
        elif op == 'not_in':
            return ~s.isin(val)
        else:
            raise ValueError(f"Unsupported operator: '{op}' for column '{col}'")


def filter_transactions(df, filter_structure, id_col='merchant_id'):
    mask = apply_filter(df, filter_structure)
    return df[mask]

def build_schema_prompt(df: pd.DataFrame) -> str:
    """
    Builds a schema string describing each column and its type for inclusion in the LLM system prompt.
    
    Args:
        df: The DataFrame containing original data columns.
        computed_columns: A dict of computed column names and their descriptions.
    
    Returns:
        A string to insert in the system prompt describing the schema.
    """
    schema_lines = ["Columns:"]
    for col, dtype in df.dtypes.items():
        schema_lines.append(f"- {col} ({dtype})")

    return "\n".join(schema_lines)