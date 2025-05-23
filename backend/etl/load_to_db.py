import pandas as pd
import json
import os
import logging
import math
from dateutil.parser import parse as date_parse
from backend.db.supabase_client import supabase

logging.basicConfig(level=logging.INFO)

# On conflict keys by table (adjust if needed to match your DB schema!)
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

def clean_date(val):
    """Return ISO string if val is a valid date, else None."""
    try:
        if pd.isnull(val) or val in ("NaT", "nan", "None", "", None):
            return None
        # Try pandas to_datetime first (handles weird types)
        dt = pd.to_datetime(val, errors='coerce')
        if pd.isnull(dt):
            return None
        return dt.isoformat()
    except Exception:
        try:
            # Try generic parser for odd string dates
            dt = date_parse(str(val), fuzzy=True)
            return dt.isoformat()
        except Exception:
            return None

def fix_floats_and_dates(records, date_fields=None):
    """Cleans all rows for JSON insert, handles floats, inf, NaT, and dates."""
    if date_fields is None:
        date_fields = set()
    for row in records:
        for k, v in row.items():
            # Date cleaning (force to None if not a valid date)
            if k in date_fields:
                row[k] = clean_date(v)
            # Numeric NaN/inf
            elif isinstance(v, float):
                if math.isnan(v) or math.isinf(v):
                    row[k] = None
            # pd.Timestamp or generic date with isoformat
            elif hasattr(v, 'isoformat'):
                try:
                    row[k] = v.isoformat() if pd.notnull(v) else None
                except Exception:
                    row[k] = str(v)
            # NaT as string
            elif isinstance(v, str) and v in ['NaT', 'nan', 'inf', '-inf']:
                row[k] = None
    return records

def batch_insert(table: str, records: list, date_fields=None, batch_size: int = 500):
    if not records: return
    records = fix_floats_and_dates(records, date_fields or set())
    on_conflict = ON_CONFLICT_KEYS.get(table, None)
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        if on_conflict:
            res = supabase.table(table).upsert(batch, on_conflict=on_conflict).execute()
        else:
            res = supabase.table(table).insert(batch).execute()
        if getattr(res, "status_code", None) not in (200, 201):
            logging.error(f"Error inserting/upserting into {table}: {res}")

def safe_load_csv(path, converters=None, parse_dates=None):
    if not os.path.exists(path):
        logging.warning(f"File not found: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, converters=converters)
        # Drop all-NA columns (common in some exports)
        df = df.dropna(axis=1, how="all")
        if parse_dates:
            for col in parse_dates:
                if col in df.columns:
                    # Don't coerce yet, do it later in cleaning
                    pass
        return df
    except Exception as e:
        logging.error(f"Failed to read {path}: {e}")
        return pd.DataFrame()

def safe_json_load(x):
    try:
        return json.loads(x) if pd.notnull(x) else []
    except Exception:
        return []

def load_projects():
    path = "data/processed/project_df.csv"
    converters = {
        "topics": safe_json_load,
        "legalBasis": safe_json_load,
        "sci_voc": safe_json_load,
        "institutions": safe_json_load,
    }
    df = safe_load_csv(path, converters=converters)
    if df.empty:
        logging.warning("No projects loaded (empty DataFrame)")
        return

    rename_map = {
        "startDate": "start_date",
        "endDate": "end_date",
        "ecSignatureDate": "ec_signature_date",
        "contentUpdateDate": "content_update_date",
        "grantDoi": "grant_doi",
        "ecMaxContribution": "ec_max_contribution",
        "totalCost": "total_cost",
        "ecContribution_per_year": "ec_contribution_per_year",
        "totalCost_per_year": "total_cost_per_year",
        "frameworkProgramme": "framework_programme",
        "masterCall": "master_call",
        "subCall": "sub_call",
        "fundingScheme": "funding_scheme",
    }
    df = df.rename(columns=rename_map)

    # These are all the columns expected in DB (adjust for your DB schema!)
    project_schema = [
        "id", "acronym", "status", "title", "start_date", "end_date",
        "total_cost", "ec_max_contribution", "ec_signature_date", "framework_programme",
        "master_call", "sub_call", "funding_scheme", "nature", "objective",
        "content_update_date", "rcn", "grant_doi", "duration_days", "duration_months",
        "duration_years", "n_institutions", "coordinator_name",
        "ec_contribution_per_year", "total_cost_per_year"
    ]
    date_fields = {
        "start_date", "end_date", "ec_signature_date", "content_update_date"
    }
    db_cols = [c for c in project_schema if c in df.columns]
    projects = df[db_cols].copy()
    batch_insert("projects", projects.to_dict(orient="records"), date_fields=date_fields)

    org_df = safe_load_csv("data/processed/organization_df.csv")
    org_name_to_id = dict(zip(org_df["name"], org_df["id"])) if "name" in org_df and "id" in org_df else {}

    # Project topics
    if "topics" in df.columns:
        pt = []
        for _, row in df[["id", "topics"]].explode("topics").iterrows():
            if pd.notnull(row["topics"]) and row["topics"]:
                pt.append({"project_id": row["id"], "topic_code": row["topics"]})
        batch_insert("project_topics", pt)

    if "legalBasis" in df.columns:
        pl = []
        for _, row in df[["id", "legalBasis"]].explode("legalBasis").iterrows():
            if pd.notnull(row["legalBasis"]) and row["legalBasis"]:
                pl.append({"project_id": row["id"], "legal_basis_code": row["legalBasis"]})
        batch_insert("project_legal_basis", pl)

    if "sci_voc" in df.columns:
        sv = []
        for _, row in df[["id", "sci_voc"]].explode("sci_voc").iterrows():
            if pd.notnull(row["sci_voc"]) and row["sci_voc"]:
                sv.append({"project_id": row["id"], "sci_voc_code": row["sci_voc"]})
        batch_insert("project_sci_voc", sv)

    # Organization linkage, safely convert names if needed
    if "institutions" in df.columns:
        po = []
        for _, row in df[["id", "institutions"]].explode("institutions").iterrows():
            org = row["institutions"]
            org_id = None
            try:
                org_id = int(org)
            except Exception:
                org_id = org_name_to_id.get(org)
                try:
                    org_id = int(org_id)
                except Exception:
                    org_id = None
            if pd.notnull(org_id):
                po.append({"project_id": row["id"], "organization_id": org_id})
        batch_insert("project_organizations", po)

def load_topics():
    path = "data/processed/topics_df.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = ["code", "title"]
    cols = [c for c in schema if c in df.columns]
    
    
    if "code" not in df.columns:
        logging.error(f"Column 'code' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    if "title" not in df.columns:
        logging.error(f"Column 'title' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    
    df = df[cols]
    before = len(df)
    df = df[df["code"].notnull() & (df["code"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} topic rows with empty/null code.")

    batch_insert("topics", df[cols].to_dict(orient="records"))

def load_legal_basis():
    path = "data/processed/legal_basis_df.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = ["code", "title", "unique_programme_part"]
    cols = [c for c in schema if c in df.columns]
    
    if "code" not in df.columns:
        logging.error(f"Column 'code' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    if "title" not in df.columns:
        logging.error(f"Column 'title' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    if "unique_programme_part" not in df.columns:
        logging.error(f"Column 'unique_programme_part' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    
    
    df = df[cols]
    before = len(df)
    df = df[df["code"].notnull() & (df["code"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} legal basis rows with empty/null code.")
    
    batch_insert("legal_basis", df[cols].to_dict(orient="records"))

def load_organizations():
    path = "data/processed/organization_df.csv"
    df = safe_load_csv(path, parse_dates=["contentUpdateDate"])
    if df.empty: return
    rename_map = {"contentUpdateDate": "content_update_date"}
    df = df.rename(columns=rename_map)
    schema = [
        "id", "name", "short_name", "vat_number", "sme", "activity_type",
        "street", "post_code", "city", "country", "nuts_code", "geolocation",
        "organization_url", "contact_form", "content_update_date"
    ]
    date_fields = {"content_update_date"}
    cols = [c for c in schema if c in df.columns]
    
    df = df[cols]
    before = len(df)
    df = df[df["id"].notnull() & (df["id"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} organization rows with empty/null id.")
    
    batch_insert("organizations", df[cols].to_dict(orient="records"), date_fields=date_fields)

def load_deliverables():
    path = "data/interim/data_deliverables.csv"
    df = safe_load_csv(path, parse_dates=["contentUpdateDate"])
    if df.empty: return
    rename_map = {"contentUpdateDate": "content_update_date"}
    df = df.rename(columns=rename_map)
    schema = [
        "id", "project_id", "title", "deliverable_type", "description",
        "url", "collection", "content_update_date"
    ]
    date_fields = {"content_update_date"}
    cols = [c for c in schema if c in df.columns]
    
    df = df[cols]
    before = len(df)
    df = df[df["id"].notnull() & (df["id"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} deliverable rows with empty/null id.")
    
    batch_insert("deliverables", df[cols].to_dict(orient="records"), date_fields=date_fields)

def load_publications():
    path = "data/interim/data_publications.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = [
        "id", "project_id", "title", "is_published_as", "authors", "journal_title",
        "journal_number", "published_year", "published_pages", "issn", "isbn",
        "doi", "collection", "content_update_date"
    ]
    date_fields = {"content_update_date"}
    cols = [c for c in schema if c in df.columns]
    
    df = df[cols]
    before = len(df)
    df = df[df["id"].notnull() & (df["id"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} publication rows with empty/null id.")

    batch_insert("publications", df[cols].to_dict(orient="records"), date_fields=date_fields)

def load_sci_voc():
    path = "data/processed/sci_voc_df.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = ["code", "path", "title", "description"]
    cols = [c for c in schema if c in df.columns]

    if "code" not in df.columns:
        logging.error(f"Column 'code' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    if "path" not in df.columns:
        logging.error(f"Column 'path' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    if "title" not in df.columns:
        logging.error(f"Column 'title' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    if "description" not in df.columns:
        logging.error(f"Column 'description' missing in {path}. Columns found: {df.columns.tolist()}")
        return
    
    before = len(df)
    df = df[df["code"].notnull() & (df["code"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} sci_voc rows with empty/null code.")

    batch_insert("sci_voc", df[cols].to_dict(orient="records"))

def load_web_items():
    path = "data/processed/web_items_df.csv"
    converters = {"available_languages": safe_json_load}
    df = safe_load_csv(path, converters=converters)
    if df.empty: return
    schema = [
        "language", "available_languages", "uri", "title", "type", "source", "represents"
    ]
    cols = [c for c in schema if c in df.columns]

    df = df[cols]
    before = len(df)
    df = df[df["language"].notnull() & (df["language"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} web item rows with empty/null language.")

    batch_insert("web_items", df[cols].to_dict(orient="records"))

def load_web_links():
    path = "data/processed/web_link_df.csv"
    converters = {"available_languages": safe_json_load}
    df = safe_load_csv(path, converters=converters, parse_dates=["archivedDate"])
    if df.empty: return
    rename_map = {"archivedDate": "archived_date"}
    df = df.rename(columns=rename_map)
    schema = [
        "id", "project_id", "phys_url", "available_languages", "status", "archived_date",
        "type", "source", "represents"
    ]
    date_fields = {"archived_date"}
    cols = [c for c in schema if c in df.columns]
    
    
    df = df[cols]
    before = len(df)
    df = df[df["id"].notnull() & (df["id"].astype(str).str.strip() != "")]
    after = len(df)
    if before != after:
        logging.warning(f"Removed {before - after} web link rows with empty/null id.")
        
    
    batch_insert("web_links", df[cols].to_dict(orient="records"), date_fields=date_fields)

def main():
    load_projects()
    load_topics()
    load_legal_basis()
    load_organizations()
    load_deliverables()
    load_publications()
    load_sci_voc()
    load_web_items()
    load_web_links()
    print("Data load complete!")

if __name__ == "__main__":
    main()
