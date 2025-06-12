from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, TableData
import pandas as pd
from app.utils.router_helpers import filter_entity_data
from app.logic.branch_admins import (
        get_transaction_volume_over_time, get_customer_segmentation, get_transaction_outliers,
        get_top_customers, get_transaction_count_over_time, get_average_transaction_over_time, get_days_between_transactions
    )
from typing import List, Dict, Any
from app.utils.helpers import filter_transactions

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
        year, month, week, day, range_days, start_date, end_date
    )

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
        year, month, week, day, range_days, start_date, end_date
    )

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
        year, month, week, day, range_days, start_date, end_date
    )

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
    # Use the helper function to filter data
    df, _ = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
        year, month, week, day, range_days, start_date, end_date
    )

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_transaction_count_over_time(df, granularity, filters)


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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
        year, month, week, day, range_days, start_date, end_date
    )

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
        year, month, week, day, range_days, start_date, end_date
    )

    return get_days_between_transactions(df, filters)


@router.get("/{branch_admin_id}/export")
def export_branch_admin_data(
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
    # Use the helper function to filter data
    df, _ = filter_entity_data(
        df, "branch_admin_id", branch_admin_id,
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
        headers={"Content-Disposition": f"attachment; filename=branch_admin_{branch_admin_id}_data.csv"}
    )


@router.post("/filter", response_model=List[Dict[str, Any]])
def filter_branch_admins(
    filter_structure: Dict[str, Any] = Body(...),
    df=Depends(get_df)
):
    """
    Filter branch admins based on complex filter criteria.
    
    The filter_structure should follow the format:
    {
        "and": [
            {"column": "total_transactions", "operator": "greater_than", "value": 30},
            {"column": "unique_terminals", "operator": "greater_than", "value": 5}
        ]
    }
    
    Supported operators: equals, greater_than, less_than, between, in
    """
    try:
        # Add computed attributes with branch_admin_id as the grouping column
        filtered_df = filter_transactions(df, filter_structure, id_col='branch_admin_id')
        
        if filtered_df.empty:
            return []
        
        # Get unique branch admins with their attributes
        branch_admin_cols = ['branch_admin_id', 'avg_transaction_amount', 'total_transactions', 'unique_terminals']
        available_cols = [col for col in branch_admin_cols if col in filtered_df.columns]
        
        branch_admins = filtered_df[available_cols].drop_duplicates('branch_admin_id').to_dict(orient='records')
        return branch_admins
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


