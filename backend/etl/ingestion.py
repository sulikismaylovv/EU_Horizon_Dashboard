import csv
from pathlib import Path
import pandas as pd

def sniff_delimiter(path: Path) -> str:
    """Sample the first 2 KB and let csv.Sniffer pick ',' or ';' or '\t'."""
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        sample = f.read(2048)
    return csv.Sniffer().sniff(sample, delimiters=[',',';','\t']).delimiter

def load_csvs_from_dir(csv_path: Path) -> dict[str, pd.DataFrame]:
    """Load a single CSV and return {stem: DataFrame}"""
    sep = sniff_delimiter(csv_path)
    df = pd.read_csv(
        csv_path,
        sep=sep,
        quotechar='"',
        dtype=str,
        na_values=['', 'NA'],
        low_memory=False,
        on_bad_lines='skip'
    )
    print(f"→ Loaded file '{csv_path.name}' with shape {df.shape}")
    return {csv_path.stem: df}


