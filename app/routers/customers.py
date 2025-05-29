from fastapi import APIRouter, Depends
from ..core.data import get_df
from ..models.stats import SimpleStat

router = APIRouter(prefix="/customers", tags=["Customers"])

@router.get("/count", response_model=SimpleStat)
def total_customers(df = Depends(get_df)):
    count = df["customer_id"].nunique()
    return SimpleStat(metric="Unique Customer Count", value=count)
