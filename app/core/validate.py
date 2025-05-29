import pandas as pd
from typing import Iterable
from fastapi import HTTPException, UploadFile
from tempfile import NamedTemporaryFile

REQUIRED_COLUMNS: set[str] = {
    "transaction_id", "customer_id", "merchant_id",
    "terminal_id", "amount", "date", "channel"
}

def validate_and_stage(file: UploadFile) -> str:
    """
    Validate header & return a temp file path ready for promotion to data/.
    Raises HTTPException(422) if validation fails.
    """
    try:
        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            contents = file.file.read()
            tmp.write(contents)
            tmp_path = tmp.name
    finally:
        file.file.close()

    try:
        df_head = pd.read_csv(tmp_path, nrows=5)   # read only header+few rows
    except Exception as exc:
        raise HTTPException(422, f"CSV parse error: {exc}") from exc

    missing = REQUIRED_COLUMNS - set(df_head.columns)
    if missing:
        raise HTTPException(
            422,
            f"CSV missing required columns: {', '.join(sorted(missing))}"
        )
    return tmp_path
