# src/preprocess_data.py

from config import RAW_FILES, DATASET_GROUPS, output_path
from etl.ingestion import load_csvs_from_dir  # Will need to modify to accept a list of files
from etl.cleaning import TABLE_CLEANERS, standardize_columns
from etl.transform import (
    transform_projects,
    transform_deliverables,
    transform_summaries,
    transform_publications,
)
from utils.save_load import save_parquet

def load_and_clean(file_keys):
    """Load and clean the CSVs specified by file_keys list."""
    dfs = {}

    for key in file_keys:
        path = RAW_FILES[key]
        df_dict = load_csvs_from_dir(path) if path.exists() else None
        if df_dict is not None:
            for subkey, df in df_dict.items():
                # Match cleaner by file stem (subkey), not dataset key
                print(f"→ Loaded file '{subkey}' with shape {df.shape}")

                cleaner = TABLE_CLEANERS.get(subkey, standardize_columns)
                cleaned_df = cleaner(df)
                print(f"   – Cleaned: {cleaned_df.shape} rows")
                
                dfs[subkey] = cleaned_df  # subkey = file stem, like 'project'
    return dfs



def process_dataset(group_key):
    print(f"\n▶️  Processing dataset '{group_key}'")

    file_keys = DATASET_GROUPS[group_key]
    cleaned = load_and_clean(file_keys)

    # Write each cleaned table as interim
    for k, df in cleaned.items():
        out_path = output_path(k, stage="interim")
        save_parquet(df, out_path)
        print(f"   – Interim: {out_path.name} ({df.shape[0]} rows)")

    # Transform and join to one final table
    if group_key == "projects":
        print(f"→ Cleaned keys for group '{group_key}': {list(cleaned.keys())}")
        final = transform_projects(cleaned)
    elif group_key == "deliverables":
        final = transform_deliverables(cleaned)
    elif group_key == "summaries":
        final = transform_summaries(cleaned)
    elif group_key == "publications":
        final = transform_publications(cleaned)
    else:
        raise ValueError(f"Unknown dataset group '{group_key}'")

    # Save processed table
    out_final = output_path(group_key, stage="processed")
    save_parquet(final, out_final)
    print(f"   ✅  Processed '{group_key}': {final.shape[0]} rows → {out_final}")

if __name__ == "__main__":
    for group in DATASET_GROUPS:
        process_dataset(group)
    print("\n🎉 All datasets processed!")
