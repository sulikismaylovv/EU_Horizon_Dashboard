import pandas as pd
import json
import os
import logging
import math
from dateutil.parser import parse as date_parse
from backend.db.supabase_client import supabase

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(message)s",
    handlers=[
        logging.FileHandler("load_warnings.log"),
        logging.StreamHandler()
    ]
)

ON_CONFLICT_KEYS = {
    "projects": "id",
    "project_organizations": "project_id,organization_id",
    "project_topics": "project_id,topic_code",
    "project_legal_basis": "project_id,legal_basis_code",
    "project_sci_voc": "project_id,sci_voc_code",
    "topics": "code",
    "legal_basis": "code",
    "organizations": "id",
    "sci_voc": "code",
    "deliverables": "id",
    "publications": "id",
    "web_items": "id",
    "web_links": "id"
}

COLUMN_ALIASES = {
    "startdate": "start_date", "enddate": "end_date", "ecsignaturedate": "ec_signature_date",
    "contentupdatedate": "content_update_date", "grantdoi": "grant_doi",
    "ecmaxcontribution": "ec_max_contribution", "totalcost": "total_cost",
    "eccontribution_per_year": "ec_contribution_per_year", "totalcost_per_year": "total_cost_per_year",
    "frameworkprogramme": "framework_programme", "mastercall": "master_call",
    "subcall": "sub_call", "fundingscheme": "funding_scheme", "legalbasis": "code",
    "topic": "code", "uniqueprogrammepart": "unique_programme_part", "deliverabletype": "deliverable_type",
    "projectid": "project_id", "physurl": "phys_url", "archiveddate": "archived_date",
    "availablelanguages": "available_languages"
}

def clean_date(val):
    try:
        if pd.isnull(val) or val in ("NaT", "nan", "None", "", None):
            return None
        dt = pd.to_datetime(val, errors='coerce')
        return None if pd.isnull(dt) else dt.isoformat()
    except Exception:
        try:
            dt = date_parse(str(val), fuzzy=True)
            return dt.isoformat()
        except Exception:
            return None

def fix_floats_and_dates(records, date_fields=None):
    if date_fields is None:
        date_fields = set()
    for row in records:
        for k, v in row.items():
            if k in date_fields:
                row[k] = clean_date(v)
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
            elif hasattr(v, 'isoformat'):
                try:
                    row[k] = v.isoformat() if pd.notnull(v) else None
                except Exception:
                    row[k] = str(v)
            elif isinstance(v, str) and v in ['NaT', 'nan', 'inf', '-inf']:
                row[k] = None
    return records

def batch_insert(table, records, date_fields=None, batch_size=500):
    if not records:
        return
    records = fix_floats_and_dates(records, date_fields or set())
    on_conflict = ON_CONFLICT_KEYS.get(table)
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        if on_conflict:
            res = supabase.table(table).upsert(batch, on_conflict=on_conflict).execute()
        else:
            res = supabase.table(table).insert(batch).execute()
        if getattr(res, "status_code", None) not in (200, 201):
            logging.error(f"Error inserting/upserting into {table}: {res.json() if hasattr(res, 'json') else res}")

def safe_load_csv(path, converters=None, parse_dates=None):
    if not os.path.exists(path):
        logging.warning(f"File not found: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, converters=converters)
        df.columns = df.columns.str.strip().str.lower()
        df.rename(columns={k: v for k, v in COLUMN_ALIASES.items()}, inplace=True)
        return df.dropna(axis=1, how="all")
    except Exception as e:
        logging.error(f"Failed to read {path}: {e}")
        return pd.DataFrame()

def safe_json_load(x):
    try:
        return json.loads(x) if pd.notnull(x) else []
    except Exception:
        return []

# ---- Loaders ----

def load_topics():
    df = safe_load_csv("data/processed/topics_df.csv")
    if "code" in df.columns and "title" in df.columns:
        df = df[df["code"].notnull()].drop_duplicates(subset="code")
        batch_insert("topics", df[["code", "title"]].to_dict(orient="records"))

def load_legal_basis():
    df = safe_load_csv("data/processed/legal_basis_df.csv")
    if {"code", "title", "unique_programme_part"}.issubset(df.columns):
        df = df[df["code"].notnull()].drop_duplicates(subset="code")
        batch_insert("legal_basis", df[["code", "title", "unique_programme_part"]].to_dict(orient="records"))

def load_sci_voc():
    df = safe_load_csv("data/processed/sci_voc_df.csv")
    if {"code", "path", "title", "description"}.issubset(df.columns):
        df = df[df["code"].notnull()].drop_duplicates(subset="code")
        batch_insert("sci_voc", df[["code", "path", "title", "description"]].to_dict(orient="records"))

def load_organizations():
    df = safe_load_csv("data/processed/organization_df.csv")
    if "id" not in df.columns:
        return
    if "sme" in df.columns:
        df["sme"] = df["sme"].astype(str).str.lower().map({"true": True, "false": False, "1": True, "0": False}).fillna(False)
    df = df[df["id"].notnull()]
    keep = [
        "id", "name", "short_name", "vat_number", "sme", "activity_type",
        "street", "post_code", "city", "country", "nuts_code", "geolocation",
        "organization_url", "contact_form", "content_update_date"
    ]
    batch_insert("organizations", df[keep].to_dict(orient="records"), date_fields={"content_update_date"})

def load_projects():
    df = safe_load_csv("data/processed/project_df.csv", converters={
        "topics": safe_json_load,
        "legalbasis": safe_json_load,
        "sci_voc": safe_json_load,
        "institutions": safe_json_load,
    })
    if df.empty:
        return

    # Insert projects
    project_fields = [
        "id", "acronym", "status", "title", "start_date", "end_date",
        "total_cost", "ec_max_contribution", "ec_signature_date", "framework_programme",
        "master_call", "sub_call", "funding_scheme", "nature", "objective",
        "content_update_date", "rcn", "grant_doi", "duration_days", "duration_months",
        "duration_years", "n_institutions", "coordinator_name",
        "ec_contribution_per_year", "total_cost_per_year"
    ]
    batch_insert("projects", df[[col for col in project_fields if col in df.columns]].to_dict(orient="records"),
                 date_fields={"start_date", "end_date", "ec_signature_date", "content_update_date"})

    def fk_insert(subset_col, target_table, target_column):
        table_to_file = {
            "topics": "topics_df.csv",
            "legal_basis": "legal_basis_df.csv",
            "sci_voc": "sci_voc_df.csv",
            "organizations": "organization_df.csv"
        }
        file = table_to_file.get(target_table)
        if not file:
            logging.warning(f"No file mapping found for {target_table}")
            return
        fk_df = safe_load_csv(f"data/processed/{file}")

        if fk_df.empty:
            logging.warning(f"Foreign key table {target_table} is empty")
            return

        valid = set(fk_df[target_column].dropna().astype(str))
        exploded = df[["id", subset_col]].explode(subset_col)
        exploded = exploded[exploded[subset_col].astype(str).isin(valid)]
        skipped = exploded[~exploded[subset_col].astype(str).isin(valid)]
        if not skipped.empty:
            logging.warning(f"Skipped {len(skipped)} invalid {subset_col} references in {target_table}")
        records = [{"project_id": row["id"], f"{target_column}": row[subset_col]} for _, row in exploded.iterrows()]
        batch_insert(f"project_{target_table}", records)

    if "topics" in df.columns:
        fk_insert("topics", "topics", "code")
    if "legalbasis" in df.columns:
        fk_insert("legalbasis", "legal_basis", "code")
    if "sci_voc" in df.columns:
        fk_insert("sci_voc", "sci_voc", "code")
    if "institutions" in df.columns:
        fk_insert("institutions", "organizations", "id")

def load_deliverables():
    df = safe_load_csv("data/interim/data_deliverables.csv", parse_dates=["content_update_date"])
    df = df[df["project_id"].notnull()]
    keep = ["id", "project_id", "title", "deliverable_type", "description", "url", "collection", "content_update_date"]
    batch_insert("deliverables", df[keep].to_dict(orient="records"), date_fields={"content_update_date"})

def load_publications():
    df = safe_load_csv("data/interim/data_publications.csv")
    df = df[df["project_id"].notnull()]
    keep = [
        "id", "project_id", "title", "is_published_as", "authors", "journal_title",
        "journal_number", "published_year", "published_pages", "issn", "isbn",
        "doi", "collection", "content_update_date"
    ]
    batch_insert("publications", df[keep].to_dict(orient="records"), date_fields={"content_update_date"})

def load_web_items():
    df = safe_load_csv("data/processed/web_items_df.csv", converters={"available_languages": safe_json_load})
    df["available_languages"] = df["available_languages"].apply(lambda x: x if isinstance(x, list) else [])
    keep = ["language", "available_languages", "uri", "title", "type", "source", "represents"]
    batch_insert("web_items", df[keep].to_dict(orient="records"))

def load_web_links():
    df = safe_load_csv("data/processed/web_link_df.csv", converters={"available_languages": safe_json_load}, parse_dates=["archived_date"])
    df["available_languages"] = df["available_languages"].apply(lambda x: x if isinstance(x, list) else [])
    keep = ["id", "project_id", "phys_url", "available_languages", "status", "archived_date", "type", "source", "represents"]
    batch_insert("web_links", df[keep].to_dict(orient="records"), date_fields={"archived_date"})

def main():
    load_topics()
    load_legal_basis()
    load_sci_voc()
    load_organizations()
    load_projects()
    load_deliverables()
    load_publications()
    load_web_items()
    load_web_links()
    print("✅ Data load complete!")

if __name__ == "__main__":
    main()
