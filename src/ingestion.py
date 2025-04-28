import csv
from pathlib import Path
import pandas as pd
from src.config import RAW_DIRS

def sniff_delimiter(path: Path) -> str:
    """Sample the first 2 KB and let csv.Sniffer pick ',' or ';' or '\t'."""
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        sample = f.read(2048)
    return csv.Sniffer().sniff(sample, delimiters=[',',';','\t']).delimiter

def load_csvs_from_dir(key: str) -> dict[str, pd.DataFrame]:
    """
    Read every CSV under RAW_DIRS[key], returning a dict of DataFrames
    keyed by file-stem.
    """
    raw_dir = RAW_DIRS[key]
    data: dict[str, pd.DataFrame] = {}
    for csv_file in raw_dir.glob('*.csv'):
        sep = sniff_delimiter(csv_file)
        df = pd.read_csv(
            csv_file,
            sep=sep,
            quotechar='"',
            dtype=str,           # read everything as string first
            na_values=['', 'NA'],
            low_memory=False
        )
        data[csv_file.stem] = df
    return data
