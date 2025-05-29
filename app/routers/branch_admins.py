from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, TableData
import pandas as pd
from app.logic.branch_admins import (
        apply_branch_admin_date_filters, get_transaction_volume_over_time, get_customer_segmentation, get_transaction_outliers,
        get_top_customers, get_transaction_count_over_time, get_average_transaction_over_time, get_days_between_transactions
    )

router = APIRouter(prefix="/branch-admins", tags=["Branch Admins"])

@router.get("/count", response_model=SimpleStat)
def total_branch_admins(df = Depends(get_df)):
    count = df["branch_admin_id"].nunique()
    return SimpleStat(metric="Unique Branch Admin Count", value=count)


@router.get("/{branch_admin_id}/terminals", response_model=list[str])
def get_terminals_by_branch_admin(
    branch_admin_id: str,
    df=Depends(get_df)
):
    if "branch_admin_id" not in df.columns or "terminal_id" not in df.columns:
        raise HTTPException(status_code=500, detail="Required columns missing from dataset")

    df = df[df["branch_admin_id"] == branch_admin_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No terminals found for this branch admin")

    terminal_ids = df["terminal_id"].dropna().unique().tolist()
    return terminal_ids


@router.get("/{branch_admin_id}/overview")
def branch_admin_overview(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this branch admin")

    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

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
        "days_between_transactions": get_days_between_transactions(df, filters)
    }


@router.get("/{branch_admin_id}/average-transactions", response_model=GraphData)
def branch_admin_average_transactions(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this branch_admin")

    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

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


@router.get("/{branch_admin_id}/segmentation", response_model=TableData)
def branch_admin_customer_segmentation(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this branch admin")

    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

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


@router.get("/{branch_admin_id}/top-customers", response_model=TableData)
def top_customers_per_branch_admin(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this branch admin")

    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

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


@router.get("/{branch_admin_id}/transaction-volume", response_model=GraphData)
def branch_admin_transaction_volume(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]
    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

    if df.empty:
        raise HTTPException(status_code=404, detail="No transactions match the filters")

    return get_transaction_volume_over_time(df, granularity)


@router.get("/{branch_admin_id}/transaction-count", response_model=GraphData)
def branch_admin_transaction_count(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]
    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

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


@router.get("/{branch_admin_id}/transaction-outliers", response_model=TableData)
def branch_admin_transaction_outliers(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this branch admin")

    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

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


@router.get("/{branch_admin_id}/days-between-transactions", response_model=TableData)
def branch_admin_days_between_transactions(
    branch_admin_id: str,
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
    df = df[df["branch_admin_id"] == branch_admin_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this branch admin")

    df = apply_branch_admin_date_filters(df, year, month, week, day, range_days, start_date, end_date)

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


