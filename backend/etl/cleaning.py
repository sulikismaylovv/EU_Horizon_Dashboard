import pandas as pd

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to snake_case lowercase."""
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r'\s+', '_', regex=True)
          .str.replace(r'[^\w_]',   '',  regex=True)
    )
    return df

def clean_project(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # rename
    df = df.rename(columns={
        'startdate': 'start_date',
        'enddate':   'end_date',
        'totalcost': 'total_cost',
        'grantdoi':  'grant_doi',
    })
    # parse dates
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date']   = pd.to_datetime(df['end_date'],   errors='coerce')
    # numeric
    for col in ['total_cost', 'ec_contribution', 'budget']:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    # drop exact duplicates on primary key
    if 'id' in df:
        df = df.drop_duplicates(subset='id')
    return df

def clean_organization(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = df.rename(columns={
        'projectid': 'project_id',
        'organisationid': 'organisation_id',
        'neteccontribution': 'net_ec_contribution',
        'totalcost': 'total_cost',
        'endofparticipation': 'end_of_participation',
    })
    # numeric (comma→dot)
    for col in ['net_ec_contribution', 'total_cost']:
        if col in df:
            df[col] = (
                df[col].str.replace(',', '.', regex=False)
                       .astype(float, errors='coerce')
            )
    # parse date
    if 'end_of_participation' in df:
        df['end_of_participation'] = pd.to_datetime(
            df['end_of_participation'], errors='coerce'
        )
    return df

def clean_topics(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    return df.rename(columns={'projectid':'project_id'})

def clean_legalbasis(df: pd.DataFrame) -> pd.DataFrame:
    return standardize_columns(df)

def clean_webitem(df: pd.DataFrame) -> pd.DataFrame:
    return standardize_columns(df)

def clean_weblink(df: pd.DataFrame) -> pd.DataFrame:
    return standardize_columns(df)

# map file-stems → cleaner funcs
TABLE_CLEANERS = {
    'project':      clean_project,
    'organization': clean_organization,
    'topics':       clean_topics,
    'legalBasis':   clean_legalbasis,
    'webItem':      clean_webitem,
    'webLink':      clean_weblink,
}
