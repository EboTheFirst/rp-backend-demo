from fastapi import HTTPException
from typing import List, Dict, Any, Optional
import pandas as pd
from app.utils.helpers import apply_filter, build_schema_prompt, filter_transactions
from app.utils.router_helpers import filter_entity_data
from app.chains.intent import intent_classification_chain
from app.chains.filter_extraction import filter_extraction_chain

def apply_structured_filter(
    df: pd.DataFrame, 
    filter_structure: Dict[str, Any], 
    id_col: str, 
    attribute_cols: List[str],
) -> List[Dict[str, Any]]:
    """
    Apply a structured filter to a dataframe and return filtered entities.
    
    Args:
        df: DataFrame containing the data
        filter_structure: Dictionary containing filter criteria
        id_col: Column name for entity ID (e.g., 'agent_id', 'merchant_id')
        attribute_cols: List of columns to include in the result
    
    Returns:
        List of dictionaries containing filtered entities
        
    Raises:
        HTTPException: If filter structure is invalid
    """
    try:        
        # Apply the structured filter - get the boolean mask
        mask = apply_filter(df, filter_structure)
        
        # Apply the mask to filter the DataFrame
        filtered_df = df[mask]
        
        if filtered_df.empty:
            return []
        
        # Get available columns
        available_cols = [col for col in attribute_cols if col in filtered_df.columns]
        
        # Get unique entities with their attributes
        entities = filtered_df[available_cols].drop_duplicates(id_col).to_dict(orient='records')
        return entities
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

def apply_nl_filter(
    df: pd.DataFrame, 
    query: str, 
    id_col: str, 
    attribute_cols: List[str],
) -> List[Dict[str, Any]]:
    """
    Apply a natural language filter to a dataframe and return filtered entities.
    
    Args:
        df: DataFrame containing the data
        query: Natural language query string
        id_col: Column name for entity ID (e.g., 'agent_id', 'merchant_id')
        attribute_cols: List of columns to include in the result
    
    Returns:
        List of dictionaries containing filtered entities
        
    Raises:
        HTTPException: If filter cannot be extracted or applied
    """

    schema_prompt = build_schema_prompt(df)
    
    try:
        filter_result = filter_extraction_chain.invoke({"query": query, "schema_prompt": schema_prompt})
        print("Raw LLM output:", filter_result)
        filter_structure = filter_result["filter_object"]
                
        if not filter_structure:
            raise HTTPException(status_code=400, detail="Could not extract filtering criteria from query")
            
        # Step 3: Apply the filter
        return apply_structured_filter(df, filter_structure, id_col, attribute_cols)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")
