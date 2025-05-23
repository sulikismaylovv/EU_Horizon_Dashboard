#!/usr/bin/env python
# src/preprocess_data.py
import sys
from pathlib import Path

# Add project root to sys.path if not already there
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import argparse
import logging
from pathlib import Path

import pandas as pd

from backend.config import RAW_FILES, DATASET_GROUPS, output_path
from backend.etl.ingestion import load_csv
from backend.etl.cleaning import TABLE_CLEANERS, standardize_columns
from backend.etl.transform import (
    transform_projects,
    transform_deliverables,
    transform_summaries,
    transform_publications,
)
from backend.classes import CORDIS_data


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
        logger.info("Loaded %s (%d rows)", key, len(df))

        cleaner = TABLE_CLEANERS.get(key, standardize_columns)
        df_clean = cleaner(df)
        logger.info(
            "Cleaned %-18s rows=%6d  → Cleaned rows=%6d",
            key,
            len(df),
            len(df_clean),
        )

        dfs[key] = df_clean

    return dfs


def process(group: str, do_transform: bool):
    logger.info("Processing group '%s'", group)
    cleaned = load_and_clean(DATASET_GROUPS[group])
    logger.info("Loaded %d tables for '%s'", len(cleaned), group)

    # save interim CSVs
    for name, df in cleaned.items():
        p = output_path(name, stage="interim")
        df.to_parquet(p, index=False)
        logger.info("Saved interim %s (%d rows)", p.name, len(df))

    if not do_transform:
        logger.info("Skipping transformation for group '%s'", group)
        return

    # perform transform step if requested
    transformer_map = {
        "projects": transform_projects,
        "deliverables": transform_deliverables,
        "summaries": transform_summaries,
        "publications": transform_publications,
    }
    transformer = transformer_map.get(group)
    if transformer is None:
        logger.warning("No transformer defined for group '%s', skipping", group)
        return

    final = transformer(cleaned)
    p_final = output_path(group, stage="processed")
    final.to_parquet(p_final, index=False)
    logger.info("Saved processed %s (%d rows)", p_final.name, len(final))


def enrich(parent_dir: Path):
    logger.info("Enriching with CORDIS_data")
    cd = CORDIS_data(parent_dir=parent_dir, enrich=True)
    cd.export_dataframes(
        parent_dir / "data" / "processed", format="csv", include_all=True
    )
    logger.info("Enrichment complete")


def main():
    parser = argparse.ArgumentParser(description="Load, clean (and optionally transform) CORDIS data")
    parser.add_argument("--no-clean", dest="clean", action="store_false",
                        help="Skip the cleaning (interim) stage")
    parser.add_argument("--transform", dest="transform", action="store_true",
                        help="Run the transformation stage and save processed outputs")
    parser.add_argument("--no-enrich", dest="enrich", action="store_false",
                        help="Skip the enrichment stage")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent

    if args.clean:
        for grp in DATASET_GROUPS:
            process(grp, do_transform=args.transform)

    if args.enrich:
        enrich(root)


if __name__ == "__main__":
    main()
