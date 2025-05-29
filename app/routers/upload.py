from pathlib import Path
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse

from ..core.validate import validate_and_stage
from ..core.data import replace_dataset

router = APIRouter(prefix="/upload", tags=["Upload"])

@router.post("/", response_class=JSONResponse, status_code=201)
async def upload_transactions(file: UploadFile = File(...)):
    """
    Upload a CSV to become the new data source.
    Overwrites the existing dataset and reloads it into memory instantly.
    """
    staged_path: Path = Path(validate_and_stage(file))
    row_count = replace_dataset(staged_path)
    return {
        "message": "Dataset replaced successfully",
        "rows_loaded": row_count
    }
