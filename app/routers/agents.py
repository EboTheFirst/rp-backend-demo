from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, TableData
import pandas as pd
from app.utils.router_helpers import filter_entity_data
from app.logic.agents import (
        get_transaction_volume_over_time, get_customer_segmentation, get_transaction_outliers,
        get_top_customers, get_transaction_count_over_time, get_average_transaction_over_time, get_days_between_transactions,
        get_merchant_segmentation, get_top_merchants, get_transaction_outliers_merchants
    )

router = APIRouter(prefix="/agents", tags=["Agents"])

@router.get("/count", response_model=SimpleStat)
def total_agents(df = Depends(get_df)):
    count = df["agent_id"].nunique()
    return SimpleStat(metric="Unique Branch Admin Count", value=count)


@router.get("/{agent_id}/merchants", response_model=list[str])
def get_merchants_by_agent(
    agent_id: str,
    df=Depends(get_df)
):
    if "agent_id" not in df.columns or "merchant_id" not in df.columns:
        raise HTTPException(status_code=500, detail="Required columns missing from dataset")

    df = df[df["agent_id"] == agent_id]

    if df.empty:
        raise HTTPException(status_code=404, detail="No merchants found for this agent")

    merchant_ids = df["merchant_id"].dropna().unique().tolist()
    return merchant_ids


@router.get("/{agent_id}/overview")
def agent_overview(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return {
        "transaction_volume": get_transaction_volume_over_time(df, granularity),
        "transaction_count": get_transaction_count_over_time(df, granularity, filters),
        "average_transactions": get_average_transaction_over_time(df, granularity, filters),
        "segmentation": get_customer_segmentation(df, filters),
        "top_customers": get_top_customers(df, top_mode, top_limit, filters),
        "transaction_outliers": get_transaction_outliers(df, filters),
        "days_between_transactions": get_days_between_transactions(df, filters)
    }


@router.get("/{agent_id}/average-transactions", response_model=GraphData)
def agent_average_transactions(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_average_transaction_over_time(df, granularity, filters)


@router.get("/{agent_id}/customer-segmentation", response_model=TableData)
def agent_customer_segmentation(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_customer_segmentation(df, filters)


@router.get("/{agent_id}/merchant-segmentation", response_model=TableData)
def agent_merchant_segmentation(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_merchant_segmentation(df, filters)


@router.get("/{agent_id}/top-merchants", response_model=TableData)
def top_merchants_per_agent(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_top_merchants(df, mode, limit, filters)


@router.get("/{agent_id}/top-customers", response_model=TableData)
def top_customers_per_agent(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_top_customers(df, mode, limit, filters)


@router.get("/{agent_id}/transaction-volume", response_model=GraphData)
def agent_transaction_volume(
    agent_id: str,
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
    df, _ = filter_entity_data(
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_transaction_volume_over_time(df, granularity)


@router.get("/{agent_id}/transaction-count", response_model=GraphData)
def agent_transaction_count(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_transaction_count_over_time(df, granularity, filters)


@router.get("/{agent_id}/transaction-outliers", response_model=TableData)
def agent_transaction_outliers(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_transaction_outliers(df, filters)


@router.get("/{agent_id}/days-between-transactions", response_model=TableData)
def agent_days_between_transactions(
    agent_id: str,
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
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_days_between_transactions(df, filters)


@router.get("/{agent_id}/export")
def export_agent_data(
    agent_id: str,
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
        df, "agent_id", agent_id,
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
        headers={"Content-Disposition": f"attachment; filename=agent_{agent_id}_data.csv"}
    )


