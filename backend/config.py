# src/config.py

from pathlib import Path

# Project root
BASE_DIR = Path(__file__).resolve().parent.parent

# All raw data files are under /data/raw/projects/
RAW_DIR = BASE_DIR / "data" / "raw" 

# Output directories for interim and processed data
INTERIM_DIR = BASE_DIR / "data" / "interim"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Mapping dataset "keys" to specific filenames for pipeline logic
RAW_FILES = {
    "projects": RAW_DIR / "project.csv",
    "organizations": RAW_DIR / "organization.csv",
    "topics": RAW_DIR / "topics.csv",
    "legal_basis": RAW_DIR / "legalBasis.csv",
    "euro_sci_voc": RAW_DIR / "euroSciVoc.csv",
    "web_item": RAW_DIR / "webItem.csv",
    "web_link": RAW_DIR / "webLink.csv",

    # Next datasetS
    # These are the ones that are not in the projects folder
    "deliverables": RAW_DIR / "projectDeliverables.csv",
    "summaries": RAW_DIR / "reportSummaries.csv",
    "publications": RAW_DIR / "projectPublications.csv",
    # ... add more as needed
}

# Define logical datasets for your ETL pipeline
DATASET_GROUPS = {
    "projects":      ["projects", "organizations", "topics", "legal_basis", "euro_sci_voc", "web_item", "web_link"],
    "deliverables":  ["deliverables"],
    "summaries":     ["summaries"],
    "publications":  ["publications"],
}

# Output filenames
def output_path(dataset, stage="interim"):
    base = INTERIM_DIR if stage == "interim" else PROCESSED_DIR
    return base / f"{dataset}.parquet"
