import pandas as pd
import numpy as np
import ast
import csv
import os
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from backend.etl.ingestion import robust_csv_reader
from pathlib import Path


############################################################### Data cleaning functions

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
    series = series.astype(str).str.strip()
    series = series.str.replace(r'[^\d.-]', '', regex=True)
    series = pd.to_numeric(series, errors='coerce')
    if not allow_negative:
        series = series.where(series >= 0)
    return series

def clean_string_column(series):
    """
    Convert a pandas Series of mixed objects to strings, handling escape characters like backslashes.
    This ensures uniform string formatting and removes unintended escape sequences.
    """
    return (
        series.astype(str)              # Convert to string
              .str.encode('unicode_escape')  # Escape special characters (like \n, \t, etc.)
              .str.decode('utf-8')      # Decode back to readable escaped string
              .str.strip()              # Remove whitespace
    )

def clean_date_column(series):
    """Parse date columns robustly."""
    return pd.to_datetime(series, errors='coerce', infer_datetime_format=True)


def get_country_from_city(city_name):
    geolocator = Nominatim(user_agent="country-inference")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    try:
        location = geocode(city_name)
        if location and location.raw.get('address'):
            return location.raw['address'].get('country')
    except:
        return None
    return None

############################################################ Dataframe-specific functions

def clean_report(df: pd.DataFrame) -> pd.DataFrame:
    # Remove all-empty or all-blank rows
    df = df.dropna(how='all').reset_index(drop=True)
 
    # fill missing values 
    df['attachment'] = df['attachment'].fillna('about:blank') 
    return df.reset_index(drop=True)

def clean_project(df: pd.DataFrame) -> pd.DataFrame:

    rename_map = {
        'startdate': 'start_date',
        'enddate': 'end_date',
        'totalcost': 'total_cost',
        'ec_contribution': 'ec_contribution',
        'budget': 'budget',
        'grantdoi': 'grant_doi',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    # standardize columns

    if 'startDate' in df: df['startDate'] = clean_date_column(df['startDate'])
    if 'endDate' in df: df['endDate'] = clean_date_column(df['endDate'])
    # Numeric
    for col in ['total_cost', 'ec_contribution', 'budget', 'id']:
        if col in df: df[col] = clean_numeric_column(df[col])

    # convert to string    
    if 'status' in df.columns:
        df['status'] = clean_string_column(df['status'])

    # Remove duplicate IDs
    if 'id' in df: df = df.drop_duplicates(subset='id')

    # Remove empty ID rows
    if 'id' in df: df = df[df['id'].notna() & (df['id'] != '')]

    # Remove all-whitespace cols
    df = df.loc[:, df.columns.str.strip().str.len() > 0]

    # Reset index
    return df.reset_index(drop=True)

def clean_organization(df: pd.DataFrame) -> pd.DataFrame:

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
        if col in df: 
            df[col] = clean_numeric_column(df[col])

    # standradize dates
    if 'end_of_participation' in df: 
        df['end_of_participation'] = clean_date_column(df['end_of_participation'])

    # Remove empty org/project ids
    for k in ['organization_id', 'project_id']:
        if k in df: df = df[df[k].notna() & (df[k] != '')]



    df['vatNumber'] = df['vatNumber'].fillna('XX000000000')
    df['shortName'] = df['shortName'].fillna('XX')
    df['SME'] = df['SME'].fillna('XX')
    df['activityType'] = df['activityType'].fillna('XX')
    df['street'] = df['street'].fillna('XX')
    df['city'] = df['city'].fillna('XX')

    # set correct country values
    df.loc[df['city']=='Windhoek', 'country']='NA' # NA was probably interpreted as NaN, can also be fixed using keep_default_na=True when loading data
    df.loc[df['city']=='WINDHOEK', 'country']='NA'
    df.loc[df['city']=='Crawley', 'country']='UK'
    df.loc[df['name']=='CEREGE', 'country']='FR'
    df.loc[df['name']=='Purdue University', 'country']='US'
    df.loc[df['name']=='Rijk Zwaan', 'country']='NL'
    df['nutsCode'] = df['nutsCode'].fillna('XX')

    # Try to find geolocation from city

    df['geolocation'] = df['geolocation'].fillna('XX')

    df['organizationURL'] = df['organizationURL'].fillna('about:blank')
    df['ecContribution'] = df['ecContribution'].fillna(0.0)
    df['netEcContribution'] = df['netEcContribution'].fillna(0.0)

    df['totalCost'] = df['totalCost'].fillna(0.0)

    # get status from project_df dataframe
    # Merge project_df (with 'id') into organization_df (with 'projectID')
    # load project_df
    # get project root
    

# Assuming this script is somewhere inside your project directory
    project_root = Path(__file__).resolve().parent.parent.parent  # adjust `.parent` levels if needed
    csv_path = project_root / 'data' / 'raw' / 'project.csv'
    project_df = robust_csv_reader(str(csv_path))
    project_df['status'] = project_df['status'].astype(str).str.encode('unicode_escape').str.decode('utf-8').str.strip()
    
    ###### OLD METHOD ######
    # org_with_status = df.merge(
    #     project_df[['id', 'status']],
    #     left_on='projectID',
    #     right_on='id',
    #     how='left'
    # )

    # # from etl.cleaning import 
    # # Set 'active' to True if status is SIGNED, else False
    # org_with_status['active'] = str(org_with_status['status'].astype(str).str.encode('unicode_escape').str.decode('utf-8').str.strip()) == 'SIGNED'

    # org_with_status = org_with_status.drop(columns=['id', 'status'])

    ###### NEW METHOD ######
    # Create a mapping from project_df id to status
    status_map = project_df.set_index('id')['status']

    # Map the status to organization_df and check if it's SIGNED
    df['active'] = df['projectID'].map(status_map) == 'SIGNED'
    # De-duplication
    if 'organizationID' in df and 'projectID' in df:
        df = df.drop_duplicates(subset=['organizationID', 'projectID'])
        
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
    # Could add mappings/validation for legal basis codes here in the future.
    # For example, validate column names or map legal basis codes to standardized values.
    # cover missing values
    # All values are either True or NaN => change all missing to False
    df['uniqueprogrammepart'] = df['uniqueprogrammepart'].fillna(False)
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
    df_std = standardize_columns(df)

    # If projectid or deliverableid exists, enforce uniqueness
    if 'projectid' in df_std and 'deliverableid' in df_std:
        df_std = df_std.drop_duplicates(subset=['projectid', 'deliverableid'])

    ### Cover missing values
    # change unknown deliverable types to other
    df_std['deliverabletype'] = df_std['deliverabletype'].fillna('Other')

    # change empty descriptions to title of that particular row
    df_std['description'] = df_std['description'].fillna(df_std['title'])

    # change missing url to homepage of the particular project (only one missing)
    df_std['url'] = df_std['url'].fillna('https://selfy-project.eu/')

    # add missing rcn number (only one missing)
    df_std['rcn'] = df_std['rcn'].fillna(1077637.0)
    #rename projectid to projectID and deliverabletype to deliverableType
    df_std = df_std.rename(columns={'projectid': 'projectID', 'deliverabletype': 'deliverableType'})

    return df_std.reset_index(drop=True)


def clean_summaries(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # Remove all-blank, all-null rows
    df = df.dropna(how='all').reset_index(drop=True)
    return df

def clean_scivoc(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    # Remove all-blank, all-null rows
    df = df.dropna(how='all').reset_index(drop=True)
    
    # drop empty column euroSciVocDescription
    if 'euroSciVocDescription' in df:
        df = df[df['euroSciVocDescription'].notna() & (df['euroSciVocDescription'] != '')]
    
    # Convert 'project_id' to numeric
    if 'project_id' in df:
        df['project_id'] = pd.to_numeric(df['project_id'], errors='coerce')
    # Convert 'publication_id' to numeric
    if 'publication_id' in df:
        df['publication_id'] = pd.to_numeric(df['publication_id'], errors='coerce')
    return df.reset_index(drop=True)

def clean_publications(df: pd.DataFrame) -> pd.DataFrame:
    # Cover missing values
    # fill some gaps in the data structure
    df['isbn'] = df['isbn'].fillna('0000-0000')
    df['issn'] = df['issn'].fillna('0000-0000')
    df['publishedPages'] = df['publishedPages'].fillna(0)
    df['doi'] = df['doi'].fillna('about:blank')
    df['journalTitle'] = df['journalTitle'].fillna('Miscalleneous')
    df['journalNumber'] = df['journalNumber'].fillna(0)
    df['authors'] = df['authors'].fillna('sine nome')

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
