from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, GraphPoints
import pandas as pd


router = APIRouter(prefix="/merchants", tags=["Merchants"])

@router.get("/count", response_model=SimpleStat)
def total_merchants(df = Depends(get_df)):
    count = df["merchant_id"].nunique()
    return SimpleStat(metric="Unique Merchant Count", value=count)

@router.get("/{merchant_id}/average-transactions", response_model=GraphData)
def merchant_average_transactions(
    merchant_id: str,
    granularity: str = Query(..., pattern="^(daily|weekly|monthly|yearly)$"),
    year: int = None,
    month: int = None,
    week: int = None,
    df=Depends(get_df)
):
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["merchant_id"] == merchant_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this merchant")

    # Enrich date components
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["week"] = df["date"].dt.isocalendar().week

    # Apply filters only when they make sense
    if granularity in ("daily", "weekly", "monthly") and year:
        df = df[df["year"] == year]

    if granularity in ("daily", "monthly") and month: # Weeks cross over months that's why it isn't here
        df = df[df["month"] == month]

    if granularity == "daily" and week:
        df = df[df["week"] == week]


    if df.empty:
        raise HTTPException(status_code=404, detail="No data matches the given filters")

    # Set groupings and label formatting
    if granularity == "daily":
        group_cols = ["year", "month", "day"]
        label_fmt = lambda r: f"{int(r['year']):04d}-{int(r['month']):02d}-{int(r['day']):02d}"
    elif granularity == "weekly":
        group_cols = ["year", "week"]
        label_fmt = lambda r: f"{int(r['year']):04d}-W{int(r['week']):02d}"
    elif granularity == "monthly":
        group_cols = ["year", "month"]
        label_fmt = lambda r: f"{int(r['year']):04d}-{int(r['month']):02d}"
    else:  # yearly
        group_cols = ["year"]
        label_fmt = lambda r: f"{int(r['year']):04d}"


    # Aggregate and label
    grouped = df.groupby(group_cols)["amount"].mean().reset_index()
    grouped["label"] = grouped.apply(label_fmt, axis=1)
    grouped = grouped.sort_values(group_cols)

    # Dynamic metric label
    filter_parts = []
    if year:
        filter_parts.append(str(year))
    if month:
        filter_parts.append(f"Month {month}")
    if week and granularity == "daily":
        filter_parts.append(f"Week {week}")

    filter_text = " for " + ", ".join(filter_parts) if filter_parts else ""
    metric_label = f"{granularity.capitalize()} Average Transaction Value{filter_text}"

    return GraphData(
        metric=metric_label,
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].round(2).tolist()
        )
    )

@router.get("/export")
def export_transactions(df = Depends(get_df)):
    """Download the entire CSV as a stream."""
    buf = StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    headers = {"Content-Disposition": 'attachment; filename="transactions.csv"'}
    return StreamingResponse(iter([buf.getvalue()]),
                             media_type="text/csv",
                             headers=headers)
