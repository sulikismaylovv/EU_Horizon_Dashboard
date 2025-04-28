# src/config.py
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR_HORIZON_PROJECTS = BASE_DIR / 'data' / 'raw' / 'horizon_projects'
INTERIM_DIR_HORIZON_PROJECTS = BASE_DIR / 'data' / 'interim' / 'horizon_projects'
PROCESSED_DIR_HORIZON_PROJECTS = BASE_DIR / 'data' / 'processed' / 'horizon_projects'

# Filenames in the Horizon Projects folder
FILES = {
    'project':       'project.csv',
    'organization':  'organization.csv',
    'topics':        'topics.csv',
    'legal_basis':   'legalBasis.csv',
    'euro_sci_voc':  'euroSciVoc.csv',
    'web_item':      'webItem.csv',
    'web_link':      'webLink.csv',
}
