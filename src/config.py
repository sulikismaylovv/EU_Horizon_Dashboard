# src/config.py

# Configuration file for the Horizon Projects data pipeline. Here we define the base directories and filenames for the raw, interim, and processed data.
# Overall we have 4 folders to process data from:
# 1. raw: the original data from the Horizon Projects 
# 2. raw: the original data from the Horizon Projects Deliverabls
# 3. raw: the original data from the Horizon Report Summaries
# 4. raw: the original data from the Horizon Report Publications

# Each of these folders contains a number of CSV files that we will process and store in the interim and processed folders.
# The interim folder will contain the processed data that is not yet ready for analysis, while the processed folder will contain the final data that is ready for analysis.


from pathlib import Path

# Base project directory
tmp = Path(__file__).resolve().parent.parent

# Raw data directories per domain
RAW_DIRS = {
    'projects':       tmp / 'data' / 'raw' / 'horizon_projects',
    'deliverables':   tmp / 'data' / 'raw' / 'horizon_projects_deliverables',
    'report_summaries': tmp / 'data' / 'raw' / 'horizon_report_summaries',
    'publications':   tmp / 'data' / 'raw' / 'horizon_publications',
}

# Interim and processed output directories
INTERIM_DIRS = {d: tmp / 'data' / 'interim' / d for d in RAW_DIRS}
PROCESSED_DIRS = {d: tmp / 'data' / 'processed' / d for d in RAW_DIRS}

# Filenames in each raw directory
def make_filemap(**kwargs): return kwargs

FILES = {
    'projects': make_filemap(
        project='project.csv',
        organization='organization.csv',
        topics='topics.csv',
        legal_basis='legalBasis.csv',
        euro_sci_voc='euroSciVoc.csv',
        web_item='webItem.csv',
        web_link='webLink.csv',
    ),
    'deliverables': make_filemap(
        deliverable='deliverable.csv',
        organization='organization.csv',
    ),
    'report_summaries': make_filemap(
        summary='report_summaries.csv',
    ),
    'publications': make_filemap(
        publication='publications.csv',
        organization='organization.csv',
    ),
}

