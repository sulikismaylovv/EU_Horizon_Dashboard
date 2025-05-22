#!/usr/bin/env python
# src/preprocess_data.py

import argparse
import logging
from pathlib import Path

from config import RAW_FILES, DATASET_GROUPS, output_path
from etl.ingestion import load_csv
from etl.cleaning import TABLE_CLEANERS, standardize_columns
from etl.transform import (
    transform_projects,
    transform_deliverables,
    transform_summaries,
    transform_publications,
)
from classes import CORDIS_data
import pandas as pd

# set the root level


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def load_and_clean(group_keys: list[str]) -> dict[str, pd.DataFrame]:
    """
    Load and clean the CSVs specified by group_keys list.
    Uses the same key for path lookup and for selecting the cleaning function.
    """
    dfs: dict[str, pd.DataFrame] = {}
    for key in group_keys:
        path = RAW_FILES.get(key)
        if path is None or not path.exists():
            logger.warning("Missing raw file for '%s': %s", key, path)
            continue

        df = load_csv(path, robust=True)
        # Use the same key for cleaner lookup
        logger.info("Loaded %s (%d rows)", key, len(df))
        cleaner = TABLE_CLEANERS.get(key, standardize_columns)
        df_clean = cleaner(df)
        logger.info("Loaded %-18s rows=%6d  → Cleaned rows=%6d", key, len(df), len(df_clean))

        dfs[key] = df_clean

    return dfs


def process(group: str):
    logger.info("Processing group '%s'", group)
    cleaned = load_and_clean(DATASET_GROUPS[group])

    # Log
    logger.info("Loaded %d tables", len(cleaned))


    # save interim
    for name, df in cleaned.items():
        p = output_path(name, stage="interim")
        df.to_parquet(p, index=False)
        logger.info("Saved interim %s (%d rows)", p.name, len(df))

    # transform
    transformer = {
        "projects": transform_projects,
        "deliverables": transform_deliverables,
        "summaries": transform_summaries,
        "publications": transform_publications,
    }[group]
    final = transformer(cleaned)

    p_final = output_path(group, stage="processed")
    final.to_parquet(p_final, index=False)
    logger.info("Saved processed %s (%d rows)", p_final.name, len(final))

def enrich(parent_dir: Path):
    logger.info("Enriching with CORDIS_data")
    cd = CORDIS_data(parent_dir=parent_dir, enrich=True)
    cd.export_dataframes(parent_dir / "data" / "interim", format="csv", include_all=True)
    logger.info("Enrichment complete")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--no-clean", dest="clean", action="store_false")
    p.add_argument("--no-enrich", dest="enrich", action="store_false")
    args = p.parse_args()

    root = Path(__file__).resolve().parent.parent
    if args.clean:
        for grp in DATASET_GROUPS:
            process(grp)
    if args.enrich:
        enrich(root)

if __name__ == "__main__":
    main()
