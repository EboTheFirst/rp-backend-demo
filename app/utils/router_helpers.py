from fastapi import HTTPException
import pandas as pd
from app.utils.analytics import _apply_date_filters

def filter_entity_data(df, entity_id_col, entity_id, 
                      year=None, month=None, week=None, day=None, 
                      range_days=None, start_date=None, end_date=None):
    """
    Common function to filter dataframe by entity ID and date parameters.
    
    Args:
        df: The DataFrame to filter
        entity_id_col: Column name for the entity ID
        entity_id: The entity ID value to filter by
        year, month, week, day, range_days, start_date, end_date: Date filter parameters
        
    Returns:
        Filtered DataFrame and filters dictionary
        
    Raises:
        HTTPException: If no data is found or after filtering
    """
    # Ensure date column is datetime
    df["date"] = pd.to_datetime(df["date"])
    
    # Filter by entity ID
    df = df[df[entity_id_col] == entity_id]
    
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for this {entity_id_col.replace('_id', '')}")
    
    # Apply date filters
    df = _apply_date_filters(
        df, 
        year=year, 
        month=month, 
        week=week, 
        day=day, 
        range_days=range_days, 
        start_date=start_date, 
        end_date=end_date
    )
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No data after filtering")
    
    # Create filters dictionary for metric labels
    filters = {
        "year": year,
        "month": month,
        "week": week,
        "day": day,
        "range_days": range_days,
        "start_date": start_date,
        "end_date": end_date
    }
    
    return df, filters