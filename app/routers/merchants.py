from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, GraphPoints, TableData
import pandas as pd
from app.utils.router_helpers import filter_entity_data
from app.logic.merchants import (
    get_transaction_volume_over_time, get_customer_segmentation, get_transaction_outliers,
    get_top_customers, get_transaction_count_over_time, get_average_transaction_over_time,
    get_days_between_transactions, get_merchant_stats, get_transaction_frequency_analysis
)
from typing import List, Dict, Any
from app.utils.filter_helpers import apply_structured_filter, apply_nl_filter
from app.utils.helpers import add_computed_attributes
from math import ceil

router = APIRouter(prefix="/merchants", tags=["Merchants"])

@router.get("/count", response_model=SimpleStat)
def total_merchants(df = Depends(get_df)):
    count = df["merchant_id"].nunique()
    return SimpleStat(metric="Unique Merchant Count", value=count)

@router.get("/")
def get_all_merchants_paginated(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    sort_by: str = Query("total_amount", pattern="^(total_amount|transaction_count|merchant_id|merchant_name)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    search: str = Query(None, description="Search by merchant ID or name"),
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    """Get paginated list of all merchants with sorting and search."""
    try:
        # Apply date filters if provided
        filters = {}
        if any([year, month, week, day, range_days, start_date, end_date]):
            df, filters = filter_entity_data(
                df, None, None,  # No entity filtering for all merchants
                year, month, week, day, range_days, start_date, end_date
            )

        # Add computed attributes
        df = add_computed_attributes(df, 'merchant_id')

        # Group by merchant and calculate stats
        merchant_stats = df.groupby('merchant_id').agg({
            'amount': ['sum', 'mean', 'count'],
            'customer_id': 'nunique',
            'merchant_name': 'first'
        }).round(2)

        # Flatten column names
        merchant_stats.columns = ['total_amount', 'avg_amount', 'transaction_count', 'unique_customers', 'merchant_name']
        merchant_stats = merchant_stats.reset_index()

        # Apply search filter
        if search:
            search_mask = (
                merchant_stats['merchant_id'].str.contains(search, case=False, na=False) |
                merchant_stats['merchant_name'].str.contains(search, case=False, na=False)
            )
            merchant_stats = merchant_stats[search_mask]

        # Apply sorting
        ascending = sort_order == "asc"
        merchant_stats = merchant_stats.sort_values(by=sort_by, ascending=ascending)

        # Calculate pagination
        total_items = len(merchant_stats)
        total_pages = ceil(total_items / page_size)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        # Get page data
        page_data = merchant_stats.iloc[start_idx:end_idx].to_dict(orient='records')

        return {
            "data": page_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "total_pages": total_pages
            },
            "filters": filters,
            "sort": {
                "sort_by": sort_by,
                "sort_order": sort_order
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching merchants: {str(e)}")

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    return {
        "transaction_volume": get_transaction_volume_over_time(df, granularity, filters),
        "transaction_count": get_transaction_count_over_time(df, granularity, filters),
        "average_transactions": get_average_transaction_over_time(df, granularity, filters),
        "segmentation": get_customer_segmentation(df, filters),
        "top_customers": get_top_customers(df, top_mode, top_limit, filters),
        "transaction_outliers": get_transaction_outliers(df, filters),
        "days_between_transactions": get_days_between_transactions(df, filters),
        "transaction_frequency": get_transaction_frequency_analysis(df, filters),
        "stats": get_merchant_stats(df, filters)
    }

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    return get_merchant_stats(df, filters)

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    return get_transaction_volume_over_time(df, granularity, filters)

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    return get_transaction_count_over_time(df, granularity, filters)

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    return get_top_customers(df, mode, limit, filters)

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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
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
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    return get_days_between_transactions(df, filters)

@router.get("/{merchant_id}/transaction-frequency-analysis", response_model=TableData)
def merchant_transaction_frequency_analysis(
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
    """
    Analyze transaction frequency patterns for a specific merchant.
    Returns day of week patterns, hour of day patterns, and overall activity metrics.
    """
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "merchant_id", merchant_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    return get_transaction_frequency_analysis(df, filters)

@router.get("/{merchant_id}/export")
def export_merchant_data(
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
    # Use the helper function to filter data
    df, _ = filter_entity_data(
        df, "merchant_id", merchant_id,
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
        headers={"Content-Disposition": f"attachment; filename=merchant_{merchant_id}_data.csv"}
    )

@router.post("/filter", response_model=List[Dict[str, Any]])
def filter_merchants(
    filter_structure: Dict[str, Any] = Body(...),
    df=Depends(get_df)
):
    """
    Filter merchants based on complex filter criteria.
    
    The filter_structure should follow the format:
    {
        "and": [
            {"column": "total_transactions", "operator": "greater_than", "value": 10},
            {"column": "avg_transaction_amount", "operator": "between", "value": [50, 200]}
        ]
    }
    
    Supported operators: equals, greater_than, less_than, between, in
    """
    merchant_cols = ['merchant_id', 'avg_transaction_amount', 'total_transactions', 'unique_customers']
    return apply_structured_filter(df, filter_structure, 'merchant_id', merchant_cols)

@router.post("/nl-filter", response_model=List[Dict[str, Any]])
def nl_filter_merchants(
    query: str = Body(..., embed=True),
    df=Depends(get_df)
):
    """
    Filter merchants based on natural language query.
    
    Example queries:
    - "Show me merchants with more than 10 transactions"
    - "Find merchants with average transaction amount greater than $50"
    - "List merchants with more than 5 unique customers"
    """
    merchant_cols = ['merchant_id', 'avg_transaction_amount', 'total_transactions', 'unique_customers']
    return apply_nl_filter(df, query, 'merchant_id', merchant_cols)


