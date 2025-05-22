import csv
from pathlib import Path
import pandas as pd
import logging
from typing import Optional
from config import INTERIM_DIR, PROCESSED_DIR

log = logging.getLogger(__name__)

def sniff_delimiter(path: Path) -> str:
    """Sample the first 2 KB and let csv.Sniffer pick ',' or ';' or '\t'."""
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        sample = f.read(2048)
    return csv.Sniffer().sniff(sample, delimiters=[',',';','\t']).delimiter



def load_csv(path: Path, robust: bool=False, **kwargs) -> pd.DataFrame:
    if robust:
        return robust_csv_reader(path, **kwargs)
    sep = sniff_delimiter(path)
    df = pd.read_csv(path, sep=sep, dtype=str, na_values=["","NA"], low_memory=False)
    log.debug("Loaded %s rows from %s", df.shape, path.name)
    return df


############################################################## General CSV reading functions

def inspect_bad_lines(filepath, delimiter=";", quotechar='"', escapechar="\\", expected_columns=10):
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar, escapechar=escapechar)
        header = next(reader)
        rows = []
        bad_lines = []

        for lineno, row in enumerate(reader, start=2):  # Start at 2 because header is line 1

            # If the row has more columns than expected, log it
            if len(row) != expected_columns:
                bad_lines.append((lineno, row))
                continue

            rows.append(row)

        # Log bad lines
        if bad_lines:
            print(f"Found {len(bad_lines)} problematic lines. Displaying the first 5:")
            for lineno, bad_line in bad_lines[:5]:
                print(f"Line {lineno}: {bad_line}")

        # Create DataFrame
        return pd.DataFrame(rows, columns=header), bad_lines
    
def auto_fix_row(row, expected_columns, problemtic_column):
    """
    Adjusts rows with more columns than expected by merging specific columns.
    
    The issue always arises from the 15th column (description) being split in parts due to 
    the use of semicolon inside, which is interpreted as delimiter. 
    
    Fix this by merging rows problematic columns given by the parameter problematic_column until we arrive at expected column number 
    """
    # If the row is as expected, return it directly
    if len(row) == expected_columns:
        return row
    
    # If more columns are detected, we will merge the 'description' field
    # (15th field) with the next column until we get the correct number of columns.
    while len(row) > expected_columns:
        # Merge the 15th and 16th columns
        row[problemtic_column] += ";" + row[problemtic_column+1]
        del row[problemtic_column+1]

    # Return the adjusted row
    return row



def robust_csv_reader(path, delimiter=';', **kwargs) -> pd.DataFrame:
    """
    Read a CSV in “robust” mode: fix rows with too few or too many columns
    by padding/truncating to match the header, then return a DataFrame.
    """
    rows = []
    with open(path, newline='', encoding=kwargs.get('encoding', 'utf-8')) as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
        n_cols = len(header)

        for i, row in enumerate(reader, start=2):  # start=2 to account for header
            if len(row) < n_cols:
                # pad short rows
                row.extend([''] * (n_cols - len(row)))
            elif len(row) > n_cols:
                # truncate long rows
                row = row[:n_cols]
            rows.append(row)

    # Now build DataFrame with consistent columns
    return pd.DataFrame(rows, columns=header)
