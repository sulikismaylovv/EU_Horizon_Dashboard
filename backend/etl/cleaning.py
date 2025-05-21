import pandas as pd
import numpy as np
import ast

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to snake_case, ASCII, and drop trailing/leading whitespace."""
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r'\s+', '_', regex=True)
          .str.replace(r'[^\w_]', '', regex=True)
    )
    return df

def clean_numeric_column(series, allow_negative=False):
    """Convert series to numeric, handling commas, spaces, dashes, etc."""
    series = series.astype(str).str.replace(',', '').str.replace(' ', '')
    series = pd.to_numeric(series, errors='coerce')
    if not allow_negative:
        series = series.where(series >= 0)
    return series

def clean_date_column(series):
    """Parse date columns robustly."""
    return pd.to_datetime(series, errors='coerce', infer_datetime_format=True)

def clean_project(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    rename_map = {
        'startdate': 'start_date',
        'enddate': 'end_date',
        'totalcost': 'total_cost',
        'ec_contribution': 'ec_contribution',
        'budget': 'budget',
        'grantdoi': 'grant_doi',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    # Dates
    if 'start_date' in df: df['start_date'] = clean_date_column(df['start_date'])
    if 'end_date' in df: df['end_date'] = clean_date_column(df['end_date'])
    # Numeric
    for col in ['total_cost', 'ec_contribution', 'budget']:
        if col in df: df[col] = clean_numeric_column(df[col])
    # Remove duplicate IDs
    if 'id' in df: df = df.drop_duplicates(subset='id')
    # Remove empty ID rows
    if 'id' in df: df = df[df['id'].notna() & (df['id'] != '')]
    # Remove all-whitespace cols
    df = df.loc[:, df.columns.str.strip().str.len() > 0]
    # Reset index
    return df.reset_index(drop=True)

def clean_organization(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    rename_map = {
        'projectid': 'project_id',
        'organisationid': 'organization_id',
        'neteccontribution': 'net_ec_contribution',
        'totalcost': 'total_cost',
        'endofparticipation': 'end_of_participation',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    # Numeric
    for col in ['net_ec_contribution', 'total_cost']:
        if col in df: df[col] = clean_numeric_column(df[col])
    # Dates
    if 'end_of_participation' in df: df['end_of_participation'] = clean_date_column(df['end_of_participation'])
    # Remove empty org/project ids
    for k in ['organization_id', 'project_id']:
        if k in df: df = df[df[k].notna() & (df[k] != '')]
    # De-duplication
    if 'organization_id' in df and 'project_id' in df:
        df = df.drop_duplicates(subset=['organization_id', 'project_id'])
    return df.reset_index(drop=True)

def clean_topics(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = df.rename(columns={'projectid':'project_id'})
    if 'topic' in df:
        df['topic'] = df['topic'].str.strip().str.upper()
    if 'project_id' in df:
        df = df[df['project_id'].notna() & (df['project_id'] != '')]
    df = df.drop_duplicates()
    return df.reset_index(drop=True)

def clean_legalbasis(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # Could add mappings/validation for legal basis codes here
    return df.drop_duplicates().reset_index(drop=True)

def clean_webitem(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # Remove all-empty or all-blank rows
    df = df.dropna(how='all').reset_index(drop=True)
    return df

def clean_weblink(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # Could add URL validation
    df = df.dropna(how='all').reset_index(drop=True)
    return df

def clean_deliverables(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # If projectid or deliverableid exists, enforce uniqueness
    if 'project_id' in df and 'deliverable_id' in df:
        df = df.drop_duplicates(subset=['project_id', 'deliverable_id'])
    # Date columns
    for col in df.columns:
        if 'date' in col:
            df[col] = clean_date_column(df[col])
    return df.reset_index(drop=True)

def clean_summaries(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # Remove all-blank, all-null rows
    df = df.dropna(how='all').reset_index(drop=True)
    return df

def clean_publications(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # Numeric fields
    for col in ['project_id', 'publication_id']:
        if col in df: df = df[df[col].notna() & (df[col] != '')]
    df = df.drop_duplicates()
    return df.reset_index(drop=True)

# Map stems → cleaner funcs (ensure keys match file stems exactly!)
TABLE_CLEANERS = {
    'project': clean_project,
    'organization': clean_organization,
    'topics': clean_topics,
    'legalbasis': clean_legalbasis,
    'webitem': clean_webitem,
    'weblink': clean_weblink,
    'projectdeliverables': clean_deliverables,
    'reportsummaries': clean_summaries,
    'projectpublications': clean_publications,
}
