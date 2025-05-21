from pathlib import Path
import pandas as pd

def save_parquet(df: pd.DataFrame, path: Path):
    """Ensure directory exists and write a parquet (no index)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
