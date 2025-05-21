# Script to automate the data cleaning, preprocessing, and storage of datasets into ready-to-use formats (run initially or after new data updates).
# This script is designed to be run in a Jupyter notebook environment.
# It includes functions to load, clean, and save datasets in various formats (CSV, Parquet, etc.).
from src.config      import RAW_DIRS, INTERIM_DIRS, PROCESSED_DIRS, SUPPORTED_DATASETS
from etl.ingestion   import load_csvs_from_dir
from etl.cleaning    import TABLE_CLEANERS, standardize_columns
from etl.transform   import (
    transform_projects,
    transform_deliverables,
    transform_summaries,
    transform_publications,
)
from src.save_load   import save_parquet

def process_dataset(key: str):
    print(f"\n▶️  Processing dataset '{key}'")
    
    # 1) Ingest
    raw = load_csvs_from_dir(key)
    print(f"   • Loaded {len(raw)} tables.")
    
    # 2) Clean each table, save interim
    cleaned: dict[str, any] = {}
    for stem, df in raw.items():
        cleaner = TABLE_CLEANERS.get(stem, standardize_columns)
        cleaned_df = cleaner(df)
        cleaned[stem] = cleaned_df
        
        # write interim
        out_path = INTERIM_DIRS[key] / f"{stem}.parquet"
        save_parquet(cleaned_df, out_path)
        print(f"     – Interim: {stem}.parquet ({cleaned_df.shape[0]} rows)")
    
    # 3) Transform / join into one final table, then save processed
    if key == 'projects':
        final = transform_projects(cleaned)
    elif key == 'deliverables':
        final = transform_deliverables(cleaned)
    elif key == 'summaries':
        final = transform_summaries(cleaned)
    elif key == 'publications':
        final = transform_publications(cleaned)
    else:
        raise ValueError(f"Unknown dataset key '{key}'")
    
    out_final = PROCESSED_DIRS[key] / f"{key}.parquet"
    save_parquet(final, out_final)
    print(f"   ✅  Processed '{key}': {final.shape[0]} rows → {out_final}")

if __name__ == "__main__":
    for ds in SUPPORTED_DATASETS:
        process_dataset(ds)
    print("\n🎉 All datasets processed!")
