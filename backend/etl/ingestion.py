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

def robust_csv_reader(filepath, expected_columns=20, problematic_column=14, delimiter=";", quotechar='"', escapechar="\\"):
    """
    Reads a CSV file, automatically correcting rows with inconsistent column counts.
    """
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar, escapechar=escapechar)
        header = next(reader)  
        rows = []
        bad_lines = []

        for lineno, row in enumerate(reader, start=2):
            # Apply auto-fix if the number of columns is incorrect
            if len(row) != expected_columns:
                row = auto_fix_row(row, expected_columns, problematic_column)
                # If the correction fails, log the line
                if len(row) != expected_columns:
                    bad_lines.append((lineno, row))
                    continue

            rows.append(row)

        # Report problematic lines
        if bad_lines:
            print(f"Number of problematic lines that could not be fixed: {len(bad_lines)}")
            print("First 5 problematic lines:")
            for lineno, bad_line in bad_lines[:5]:
                print(f"Line {lineno}: {bad_line}")

        # Create DataFrame
        return pd.DataFrame(rows, columns=header)