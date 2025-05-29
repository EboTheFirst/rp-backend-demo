from pydantic import BaseModel
from typing import Union, List, Dict, Any
from pydantic import BaseModel

class SimpleStat(BaseModel):
    metric: str
    value: float

class GraphPoints(BaseModel):
    labels: List[str]
    values: List[float]

class GraphData(BaseModel):
    metric: str
    data: GraphPoints
    
class TableData(BaseModel):
    metric: str
    data: Union[List[Dict[str, Any]], Dict[str, Any]]

