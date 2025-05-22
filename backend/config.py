# backend/config.py

from pathlib import Path

BASE_DIR: Path = Path(__file__).resolve().parent.parent
RAW_DIR: Path = BASE_DIR / "data" / "raw"
INTERIM_DIR: Path = BASE_DIR / "data" / "interim"
PROCESSED_DIR: Path = BASE_DIR / "data" / "processed"

# Default fill values for cleaning routines
DEFAULT_FILL_STR: str = ""
DEFAULT_FILL_NUM: int = 0

# Ensure directories exist
for d in (INTERIM_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

RAW_FILES: dict[str, Path] = {
    "project": RAW_DIR / "project.csv",
    "organization": RAW_DIR / "organization.csv",
    "topics": RAW_DIR / "topics.csv",
    "legalbasis": RAW_DIR / "legalBasis.csv",
    "euroSciVoc": RAW_DIR / "euroSciVoc.csv",
    "webitem": RAW_DIR / "webItem.csv",
    "weblink": RAW_DIR / "webLink.csv",
    "projectdeliverables": RAW_DIR / "projectDeliverables.csv",
    "reportsummaries": RAW_DIR / "reportSummaries.csv",
    "projectpublications": RAW_DIR / "projectPublications.csv",
}

DATASET_GROUPS: dict[str, list[str]] = {
    "projects":      ["project","organization","topics","legalbasis","euroSciVoc","webitem","weblink"],
    "deliverables":  ["projectdeliverables"],
    "summaries":     ["reportsummaries"],
    "publications":  ["projectpublications"],
}

def output_path(dataset: str, stage: str="interim") -> Path:
    base = INTERIM_DIR if stage=="interim" else PROCESSED_DIR
    return base / f"{dataset}.parquet"
