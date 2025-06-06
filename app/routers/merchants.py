from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, GraphPoints, TableData
import pandas as pd
from app.utils.helpers import get_filter_suffix
from app.logic.merchants import (
        apply_merchant_date_filters, get_transaction_volume_over_time, get_customer_segmentation, get_transaction_outliers,
        get_top_customers, get_transaction_count_over_time, get_average_transaction_over_time, get_days_between_transactions,
        get_merchant_stats
    )
from typing import List


router = APIRouter(prefix="/merchants", tags=["Merchants"])

@router.get("/count", response_model=SimpleStat)
def total_merchants(df = Depends(get_df)):
    count = df["merchant_id"].nunique()
    return SimpleStat(metric="Unique Merchant Count", value=count)


@router.get("/{merchant_id}/terminals", response_model=list[str])
def get_terminals_by_merchant(
    merchant_id: str,
    df=Depends(get_df)
):
    if "merchant_id" not in df.columns or "terminal_id" not in df.columns:
        raise HTTPException(status_code=500, detail="Required columns missing from dataset")

    df = df[df["merchant_id"] == merchant_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No terminals found for this merchant")

    terminal_ids = df["terminal_id"].dropna().unique().tolist()
    return terminal_ids


@router.get("/{merchant_id}/branch-admins", response_model=list[str])
def get_branch_admins_by_merchant(
    merchant_id: str,
    df=Depends(get_df)
):
    if "merchant_id" not in df.columns or "branch_admin_id" not in df.columns:
        raise HTTPException(status_code=500, detail="Required columns missing from dataset")

    df = df[df["merchant_id"] == merchant_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No branch admins found for this merchant")

    branch_admin_ids = df["branch_admin_id"].dropna().unique().tolist()
    return branch_admin_ids

@router.get("/{merchant_id}/stats", response_model=List[SimpleStat])
def merchant_stats(
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

    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")

    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date
    }

    return get_merchant_stats(df, filters)



@router.get("/{merchant_id}/overview")
def merchant_overview(
    merchant_id: str,
    granularity: str = Query("monthly", pattern="^(daily|weekly|monthly|yearly)$"),
    top_mode: str = Query("amount", pattern="^(amount|count)$"),
    top_limit: int = Query(10, ge=1),
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

    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")

    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date
    }

    return {
        "transaction_volume": get_transaction_volume_over_time(df, granularity),
        "transaction_count": get_transaction_count_over_time(df, granularity, filters),
        "average_transactions": get_average_transaction_over_time(df, granularity, filters),
        "segmentation": get_customer_segmentation(df, filters),
        "top_customers": get_top_customers(df, top_mode, top_limit, filters),
        "transaction_outliers": get_transaction_outliers(df, filters),
        "days_between_transactions": get_days_between_transactions(df, filters),
        "stats": get_merchant_stats(df, filters)  # ✅ Added here
    }



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

    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data matches the given filters")

    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date
    }

    return get_average_transaction_over_time(df, granularity, filters)


@router.get("/{merchant_id}/segmentation", response_model=TableData)
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

    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No transactions match the filters")

    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date,
    }

    return get_customer_segmentation(df, filters)


@router.get("/{merchant_id}/top-customers", response_model=TableData)
def top_customers_per_merchant(
    merchant_id: str,
    mode: str = Query(..., pattern="^(amount|count)$"),
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
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["merchant_id"] == merchant_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this merchant")

    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")

    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date
    }

    return get_top_customers(df, mode, limit, filters)


@router.get("/{merchant_id}/transaction-volume", response_model=GraphData)
def merchant_transaction_volume(
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
    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No transactions match the filters")

    return get_transaction_volume_over_time(df, granularity)


@router.get("/{merchant_id}/transaction-count", response_model=GraphData)
def merchant_transaction_count(
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
    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No transactions match the filters")

    return get_transaction_count_over_time(df, granularity, {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date,
    })


@router.get("/{merchant_id}/transaction-outliers", response_model=TableData)
def merchant_transaction_outliers(
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

    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")

    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date
    }

    return get_transaction_outliers(df, filters)


@router.get("/{merchant_id}/days-between-transactions", response_model=TableData)
def merchant_days_between_transactions(
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

    df = apply_merchant_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")

    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date
    }

    return get_days_between_transactions(df, filters)


# @router.get("/export")
# def export_transactions(df = Depends(get_df)):
#     """Download the entire CSV as a stream."""
#     buf = StringIO()
#     df.to_csv(buf, index=False)
#     buf.seek(0)
#     headers = {"Content-Disposition": 'attachment; filename="transactions.csv"'}
#     return StreamingResponse(iter([buf.getvalue()]),
#                              media_type="text/csv",
#                              headers=headers)
