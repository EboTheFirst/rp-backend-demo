import threading
from typing import Optional

import pandas as pd
from fastapi import HTTPException

from .config import settings
from app.utils.caching import timed_cache

# ─── In-memory DataFrame cache ─────────────────────────────────────────────
_df: Optional[pd.DataFrame] = None
_lock = threading.Lock()        # <- single-process upload lock

# ─── Loader helpers ────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    """Initial or forced load from the canonical CSV_PATH."""
    if not settings.csv_path.exists():
        raise HTTPException(500, "Data file not found; upload a CSV first.")
    return _load_from_path(settings.csv_path)

def _load_from_path(path) -> pd.DataFrame:
    global _df
    try:
        _df = pd.read_csv(path)
    except Exception as exc:
        raise HTTPException(422, f"Could not read CSV: {exc}") from exc
    return _df

@timed_cache(seconds=60)  # Cache for 1 minute
def get_df() -> pd.DataFrame:
    """Return cached DataFrame or lazy-load."""
    return _df if _df is not None else load_data()

# ─── Called by /upload ─────────────────────────────────────────────────────
def replace_dataset(src_path) -> int:
    """
    Atomically replace the canonical CSV with src_path.
    Returns new row count.
    """
    with _lock:
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        settings.csv_path.unlink(missing_ok=True)
        src_path.replace(settings.csv_path)        # overwrite
        df = _load_from_path(settings.csv_path)    # reload into memory
    return len(df)
