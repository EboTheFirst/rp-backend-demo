from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, TableData
import pandas as pd
from app.utils.router_helpers import filter_entity_data
from app.logic.terminals import (
    get_transaction_volume_over_time, get_customer_segmentation, get_transaction_outliers,
    get_top_customers, get_transaction_count_over_time, get_average_transaction_over_time, 
    get_days_between_transactions
)

router = APIRouter(prefix="/terminals", tags=["Terminals"])

@router.get("/count", response_model=SimpleStat)
def total_terminals(df = Depends(get_df)):
    count = df["terminal_id"].nunique()
    return SimpleStat(metric="Unique Terminal Count", value=count)


@router.get("/{terminal_id}/overview")
def terminal_overview(
    terminal_id: str,
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return {
        "transaction_volume": get_transaction_volume_over_time(df, granularity, filters),
        "transaction_count": get_transaction_count_over_time(df, granularity, filters),
        "average_transactions": get_average_transaction_over_time(df, granularity, filters),
        "segmentation": get_customer_segmentation(df, filters),
        "top_customers": get_top_customers(df, top_mode, top_limit, filters),
        "transaction_outliers": get_transaction_outliers(df, filters),
        "days_between_transactions": get_days_between_transactions(df, filters)
    }


@router.get("/{terminal_id}/average-transactions", response_model=GraphData)
def terminal_average_transactions(
    terminal_id: str,
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_average_transaction_over_time(df, granularity, filters)


@router.get("/{terminal_id}/segmentation", response_model=TableData)
def terminal_customer_segmentation(
    terminal_id: str,
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_customer_segmentation(df, filters)


@router.get("/{terminal_id}/top-customers", response_model=TableData)
def top_customers_per_terminal(
    terminal_id: str,
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_top_customers(df, mode, limit, filters)


@router.get("/{terminal_id}/transaction-volume", response_model=GraphData)
def terminal_transaction_volume(
    terminal_id: str,
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_transaction_volume_over_time(df, granularity, filters)


@router.get("/{terminal_id}/transaction-count", response_model=GraphData)
def terminal_transaction_count(
    terminal_id: str,
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_transaction_count_over_time(df, granularity, filters)


@router.get("/{terminal_id}/transaction-outliers", response_model=TableData)
def terminal_transaction_outliers(
    terminal_id: str,
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_transaction_outliers(df, filters)


@router.get("/{terminal_id}/days-between-transactions", response_model=TableData)
def terminal_days_between_transactions(
    terminal_id: str,
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_days_between_transactions(df, filters)


@router.get("/{terminal_id}/export")
def export_terminal_data(
    terminal_id: str,
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    # Use the helper function to filter data
    df, _ = filter_entity_data(
        df, "terminal_id", terminal_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    # Convert to CSV
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    # Return as downloadable file
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=terminal_{terminal_id}_data.csv"}
    )
