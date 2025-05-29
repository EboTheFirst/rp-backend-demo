from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, GraphPoints
import pandas as pd
from app.utils.helpers import apply_merchant_date_filters


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
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["merchant_id"] == merchant_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this merchant")

    # Optional date range filter
    if start_date and end_date:
        try:
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df["date"] >= start) & (df["date"] <= end)]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid date format for start_date or end_date")
    elif range_days:
        end = pd.Timestamp.today().normalize()
        start = end - pd.Timedelta(days=range_days)
        df = df[(df["date"] >= start) & (df["date"] <= end)]

    if df.empty:
        raise HTTPException(status_code=404, detail="No transactions match the date filters")
    

    # Enrich date components
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["week"] = df["date"].dt.isocalendar().week

    # Apply filters only when they make sense
    if granularity in ("daily", "weekly", "monthly") and year:
        df = df[df["year"] == year]

    if granularity in ("daily", "monthly") and month:
        df = df[df["month"] == month]

    if granularity in ("daily", "weekly") and week:
        df = df[df["week"] == week]

    if granularity == "daily" and day:
        df = df[df["day"] == day]

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
    if day and granularity == "daily":
        filter_parts.append(f"Day {day}")

    filter_text = " for " + ", ".join(filter_parts) if filter_parts else ""
    metric_label = f"{granularity.capitalize()} Average Transaction Value{filter_text}"

    return GraphData(
        metric=metric_label,
        data=GraphPoints(
            labels=grouped["label"].tolist(),
            values=grouped["amount"].round(2).tolist()
        )
    )


@router.get("/{merchant_id}/segmentation")
@router.get("/{merchant_id}/segmentation")
def merchant_customer_segmentation(
    merchant_id: str,
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
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

    # Date filtering
    if start_date and end_date:
        try:
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df["date"] >= start) & (df["date"] <= end)]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid date format for start_date or end_date")
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

    if df.empty:
        raise HTTPException(status_code=404, detail="No transactions match the filters")

    # Aggregate customer spending
    customer_spend = (
        df.groupby("customer_id")["amount"].sum().reset_index()
        .sort_values(by="amount", ascending=False)
    )

    # Segmentation logic (could later be made dynamic)
    high_value = customer_spend[customer_spend["amount"] > 800]
    mid_value = customer_spend[(customer_spend["amount"] <= 800) & (customer_spend["amount"] > 500)]
    low_value = customer_spend[customer_spend["amount"] <= 500]

    return {
        "merchant_id": merchant_id,
        "filters": {
            "year": year,
            "month": month,
            "week": week,
            "day": day,
            "range_days": range_days,
            "start_date": start_date,
            "end_date": end_date
        },
        "segmentation": {
            "high_value": high_value.to_dict(orient="records"),
            "mid_value": mid_value.to_dict(orient="records"),
            "low_value": low_value.to_dict(orient="records")
        }
    }


@router.get("/top-spenders")
def top_customers_per_merchant(
    limit: int = Query(10, ge=1),
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")

    grouped = (
        df.groupby(["merchant_id", "customer_id"])["amount"].sum()
        .reset_index()
        .sort_values(by=["merchant_id", "amount"], ascending=[True, False])
        .groupby("merchant_id")
        .head(limit)
    )

    return grouped.to_dict(orient="records")

@router.get("/top-customers")
def top_customers_per_merchant(
    mode: str = Query(..., pattern="^(amount|count)$", description="Use 'amount' for top spenders, 'count' for most frequent customers."),
    limit: int = Query(10, ge=1),
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")

    if mode == "amount":
        grouped = (
            df.groupby(["merchant_id", "customer_id"])["amount"].sum()
            .reset_index()
            .sort_values(by=["merchant_id", "amount"], ascending=[True, False])
            .groupby("merchant_id")
            .head(limit)
        )
    else:  # mode == "count"
        grouped = (
            df.groupby(["merchant_id", "customer_id"])["amount"].count()
            .reset_index()
            .rename(columns={"amount": "transaction_count"})
            .sort_values(by=["merchant_id", "transaction_count"], ascending=[True, False])
            .groupby("merchant_id")
            .head(limit)
        )

    return {
        "mode": mode,
        "limit": limit,
        "filters": {
            "year": year, "month": month, "week": week, "day": day,
            "range_days": range_days, "start_date": start_date, "end_date": end_date
        },
        "results": grouped.to_dict(orient="records")
    }

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
