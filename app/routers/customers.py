from fastapi import APIRouter, Depends, HTTPException, Query, Body
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, TableData
import pandas as pd
from app.utils.router_helpers import filter_entity_data
from app.utils.helpers import filter_transactions, add_computed_attributes
from typing import List, Dict, Any
from math import ceil

router = APIRouter(prefix="/customers", tags=["Customers"])

@router.get("/count", response_model=SimpleStat)
def total_customers(df = Depends(get_df)):
    count = df["customer_id"].nunique()
    return SimpleStat(metric="Unique Customer Count", value=count)

@router.get("/")
def get_all_customers_paginated(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    sort_by: str = Query("total_amount", pattern="^(total_amount|transaction_count|customer_id|customer_name)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    search: str = Query(None, description="Search by customer ID or name"),
    year: int = None,
    month: int = None,
    week: int = None,
    day: int = Query(None, ge=1, le=31),
    range_days: int = Query(None, ge=1),
    start_date: str = None,
    end_date: str = None,
    df=Depends(get_df)
):
    """Get paginated list of all customers with sorting and search."""
    try:
        # Apply date filters if provided
        filters = {}
        if any([year, month, week, day, range_days, start_date, end_date]):
            df, filters = filter_entity_data(
                df, None, None,  # No entity filtering for all customers
                year, month, week, day, range_days, start_date, end_date
            )

        # Add computed attributes
        df = add_computed_attributes(df)

        # Group by customer and calculate stats
        customer_stats = df.groupby('customer_id').agg({
            'amount': ['sum', 'mean', 'count'],
            'merchant_id': 'nunique',
            'customer_name': 'first'
        }).round(2)

        # Flatten column names
        customer_stats.columns = ['total_amount', 'avg_amount', 'transaction_count', 'unique_merchants', 'customer_name']
        customer_stats = customer_stats.reset_index()

        # Apply search filter
        if search:
            search_mask = (
                customer_stats['customer_id'].str.contains(search, case=False, na=False) |
                customer_stats['customer_name'].str.contains(search, case=False, na=False)
            )
            customer_stats = customer_stats[search_mask]

        # Apply sorting
        ascending = sort_order == "asc"
        customer_stats = customer_stats.sort_values(by=sort_by, ascending=ascending)

        # Calculate pagination
        total_items = len(customer_stats)
        total_pages = ceil(total_items / page_size)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        # Get page data
        page_data = customer_stats.iloc[start_idx:end_idx].to_dict(orient='records')

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
        raise HTTPException(status_code=500, detail=f"Error fetching customers: {str(e)}")

@router.post("/filter", response_model=List[Dict[str, Any]])
def filter_customers(
    filter_structure: Dict[str, Any] = Body(...),
    df=Depends(get_df)
):
    """
    Filter customers based on complex filter criteria.
    
    The filter_structure should follow the format:
    {
        "and": [
            {"column": "total_transactions", "operator": "greater_than", "value": 5},
            {"column": "avg_transaction_amount", "operator": "between", "value": [100, 500]}
        ]
    }
    
    Supported operators: equals, greater_than, less_than, between, in
    """
    try:
        # Add computed attributes with customer_id as the grouping column
        filtered_df = filter_transactions(df, filter_structure, id_col='customer_id')
        
        if filtered_df.empty:
            return []
        
        # Get unique customers with their attributes
        customer_cols = ['customer_id', 'avg_transaction_amount', 'total_transactions']
        available_cols = [col for col in customer_cols if col in filtered_df.columns]
        
        customers = filtered_df[available_cols].drop_duplicates('customer_id').to_dict(orient='records')
        return customers
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
