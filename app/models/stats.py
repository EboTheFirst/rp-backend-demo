from pydantic import BaseModel
from typing import List


class SimpleStat(BaseModel):
    metric: str
    value: float

class GraphPoints(BaseModel):
    labels: List[str]
    values: List[float]

class GraphData(BaseModel):
    metric: str
    data: GraphPoints

