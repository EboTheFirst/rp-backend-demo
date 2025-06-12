from io import StringIO
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from typing import List, Dict, Any
from math import ceil

from app.utils.helpers import add_computed_attributes
from ..core.data import get_df
from ..models.stats import SimpleStat, GraphData, TableData
from app.utils.router_helpers import filter_entity_data
from app.logic.agents import (
        get_transaction_volume_over_time, get_customer_segmentation, get_transaction_outliers,
        get_top_customers, get_transaction_count_over_time, get_average_transaction_over_time, get_days_between_transactions,
        get_merchant_segmentation, get_top_merchants, get_transaction_outliers_merchants, get_merchant_activity_heatmap
    )
from app.utils.filter_helpers import apply_structured_filter, apply_nl_filter
from typing import List, Dict, Any
from app.chains.subject_column_indentifier import group_by_extraction_chain
from app.chains.intent import intent_classification_chain
import asyncio
from functools import partial

router = APIRouter(prefix="/agents", tags=["Agents"])

@router.get("/count", response_model=SimpleStat)
def total_agents(df = Depends(get_df)):
    count = df["agent_id"].nunique()
    return SimpleStat(metric="Unique Branch Admin Count", value=count)


@router.get("/list")
def list_agents(df = Depends(get_df)):
    """Get list of all available agents"""
    if "agent_id" not in df.columns:
        raise HTTPException(status_code=500, detail="Agent ID column not found in dataset")

    agents = df["agent_id"].dropna().unique().tolist()

    # Return list of agent objects with id and name
    return [
        {
            "id": agent_id,
            "name": f"Agent {agent_id}"
        }
        for agent_id in sorted(agents)
    ]


@router.get("/{agent_id}/stats", response_model=list[SimpleStat])
def agent_stats(
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

    from app.utils.analytics import _get_filter_suffix
    suffix = _get_filter_suffix(filters)

    stats = [
        SimpleStat(metric=f"Total Transaction Value{suffix}", value=round(df["amount"].sum(), 2)),
        SimpleStat(metric=f"Average Transaction Value{suffix}", value=round(df["amount"].mean(), 2)),
        SimpleStat(metric=f"Transaction Count{suffix}", value=int(df["amount"].count())),
        SimpleStat(metric=f"Unique Customers{suffix}", value=int(df["customer_id"].nunique())),
        SimpleStat(metric=f"Unique Merchants{suffix}", value=int(df["merchant_id"].nunique())),
        SimpleStat(metric=f"Unique Terminals{suffix}", value=int(df["terminal_id"].nunique())),
    ]

    return stats





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


@router.get("/{agent_id}/customers")
def get_agent_customers_paginated(
    agent_id: str,
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
    """Get paginated list of all customers for a specific agent with sorting and search."""
    # Filter data for the agent
    df, filters = filter_entity_data(
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    if df.empty:
        return {
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": 0,
                "total_pages": 0
            },
            "filters": filters
        }

    # Aggregate customer data
    customer_stats = df.groupby('customer_id').agg({
        'amount': ['sum', 'count']
    }).reset_index()

    # Flatten column names
    customer_stats.columns = ['customer_id', 'total_amount', 'transaction_count']

    # Add empty customer_name column for consistency with frontend
    customer_stats['customer_name'] = None

    # Apply search filter
    if search:
        search_mask = customer_stats['customer_id'].str.contains(search, case=False, na=False)
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


@router.get("/{agent_id}/merchants")
def get_agent_merchants_paginated(
    agent_id: str,
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
    """Get paginated list of all merchants for a specific agent with sorting and search."""
    # Filter data for the agent
    df, filters = filter_entity_data(
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )

    if df.empty:
        return {
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": 0,
                "total_pages": 0
            },
            "filters": filters
        }

    # Aggregate merchant data
    merchant_stats = df.groupby('merchant_id').agg({
        'amount': ['sum', 'count'],
        'merchant_name': 'first'  # Get merchant name
    }).reset_index()

    # Flatten column names
    merchant_stats.columns = ['merchant_id', 'total_amount', 'transaction_count', 'merchant_name']

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


@router.get("/{agent_id}/merchant-activity-heatmap")
def agent_merchant_activity_heatmap(
    agent_id: str,
    granularity: str = Query("monthly", pattern="^(daily|weekly|monthly|yearly)$"),
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
    Generate a heatmap of transaction volumes and values across merchants for an agent.
    Returns data suitable for creating heatmaps of transaction volume, count, and average value.
    """
    # Use the helper function to filter data
    df, filters = filter_entity_data(
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this agent with the given filters")
    
    return get_merchant_activity_heatmap(df, granularity, filters)


attribute_cols_base = [
        "avg_transaction_amount",
        "total_transactions",
        "sum_transaction_amount",
        "min_transaction_amount",
        "max_transaction_amount",
        "std_transaction_amount",
        "unique_customers",
        "unique_merchants",
        "unique_branch_admins",
        "unique_terminals"
    ]


@router.post("/{agent_id}/filter", response_model=List[Dict[str, Any]])
def filter_agent_merchants(
    agent_id: str,
    filter_structure: Dict[str, Any] = Body(...),
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
    Filter merchants associated with a specific agent based on complex filter criteria.
    
    The filter_structure should follow the format:
    {
        "and": [
            {"column": "total_transactions", "operator": "greater_than", "value": 50},
            {"column": "unique_customers", "operator": "greater_than", "value": 10}
        ]
    }
    
    Supported operators: equals, greater_than, less_than, between, in
    """
    
    # Step 1: Get all transactions for this agent
    df, _ = filter_entity_data(
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )
        
    # Step 2: Add computed attributes for merchants (aggregating by merchant_id)
    merchant_df = add_computed_attributes(df, 'merchant_id')
    
    attribute_cols = ["merchant_id"] + attribute_cols_base
        
    # Step 3: Apply filters to find merchants matching criteria
    return apply_structured_filter(merchant_df, filter_structure, 'merchant_id', attribute_cols)


@router.post("/{agent_id}/nl-filter", response_model=List[Dict[str, Any]])
async def nl_filter_agent_data(
    agent_id: str,
    query: str = Body(..., embed=True),
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
    Filter data for a specific agent based on natural language query.
    
    Example queries:
    - "Show me transactions with more than $100"
    - "Find customers who made purchases last week"
    - "List merchants with more than 5 transactions"
    """
    df, _ = filter_entity_data(
        df, "agent_id", agent_id,
        year, month, week, day, range_days, start_date, end_date
    )
    
    available_entity_id_columns = """
    - merchant_id
    - branch_admin_id
    - terminal_id
    - customer_id
    """
    
    # Run intent classification and group by extraction in parallel
    intent_task = asyncio.create_task(
        intent_classification_chain.ainvoke({"query": query})
    )
    group_by_task = asyncio.create_task(
        group_by_extraction_chain.ainvoke({"query": query, "available_entity_id_columns": available_entity_id_columns})
    )
    
    # Wait for both tasks to complete
    intent_result, group_by_result = await asyncio.gather(intent_task, group_by_task)
    print(intent_result)
    print(group_by_result)
    
    group_by_column = group_by_result["group_by_column"]
    
    df = add_computed_attributes(df, group_by_column)
    # print("Computed:")
    # print(df)
    
    attribute_cols = [group_by_column] + attribute_cols_base
    
    if not intent_result["filter_intent"]:
        raise HTTPException(status_code=400, detail="Could not determine filtering criteria from query")
    
    return apply_nl_filter(df, query, group_by_column, attribute_cols)
