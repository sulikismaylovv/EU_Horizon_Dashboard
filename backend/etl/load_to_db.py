import pandas as pd
import json
import os
import logging
from backend.db.supabase_client import supabase

logging.basicConfig(level=logging.INFO)

def make_json_safe(records):
    """Convert any pandas Timestamp or datetime objects in dicts to isoformat strings."""
    for row in records:
        for k, v in row.items():
            if isinstance(v, (pd.Timestamp, pd.DatetimeTZDtype)):
                row[k] = v.isoformat() if pd.notnull(v) else None
            elif hasattr(v, 'isoformat'):
                try:
                    row[k] = v.isoformat()
                except Exception:
                    row[k] = str(v)
    return records

import math

def fix_floats_and_dates(records):
    """Convert pandas NaN, inf, -inf, NaT, and pd.Timestamp etc. to JSON-safe values (None or ISO string)."""
    import math
    for row in records:
        for k, v in row.items():
            # Handle NaN, inf, -inf
            if isinstance(v, float):
                if math.isnan(v) or math.isinf(v):
                    row[k] = None
            # Handle pandas Timestamp, datetime, and NaT
            elif hasattr(v, 'isoformat'):
                try:
                    if pd.isnull(v) or str(v) == 'NaT':
                        row[k] = None
                    else:
                        row[k] = v.isoformat()
                except Exception:
                    row[k] = str(v)
            # Explicitly catch 'NaT' string
            elif isinstance(v, str) and v == 'NaT':
                row[k] = None
    return records



def batch_insert(table: str, records: list, batch_size: int = 500):
    """Insert records into a Supabase table in batches, with logging."""
    records = fix_floats_and_dates(records)
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        res = supabase.table(table).insert(batch).execute()
        if getattr(res, "status_code", None) not in (200, 201):
            logging.error(f"Error inserting into {table}: {res}")


def safe_load_csv(path, converters=None, parse_dates=None):
    if not os.path.exists(path):
        logging.warning(f"File not found: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, converters=converters)
        # Parse dates safely
        if parse_dates:
            for col in parse_dates:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
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
 # Should start with 'service_role'

    
    
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

    date_map = {
        "startDate": "start_date",
        "endDate": "end_date",
        "ecSignatureDate": "ec_signature_date",
        "contentUpdateDate": "content_update_date"
    }
    for old, new in date_map.items():
        if old in df.columns:
            df[old] = pd.to_datetime(df[old], errors='coerce')

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

    project_schema = [
        "id", "acronym", "status", "title", "start_date", "end_date",
        "total_cost", "ec_max_contribution", "ec_signature_date", "frameworkProgramme",
        "masterCall", "subCall", "fundingScheme", "nature", "objective",
        "content_update_date", "rcn", "grant_doi", "duration_days", "duration_months",
        "duration_years", "n_institutions", "coordinator_name",
        "ec_contribution_per_year", "total_cost_per_year"
    ]
    db_cols = [c for c in project_schema if c in df.columns]
    projects = df[db_cols].copy()
    batch_insert("projects", projects.to_dict(orient="records"))

    if "topics" in df.columns:
        pt = []
        for _, row in df[["id", "topics"]].explode("topics").iterrows():
            if pd.notnull(row["topics"]) and row["topics"]:
                pt.append({"project_id": row["id"], "topic_code": row["topics"]})
        if pt:
            batch_insert("project_topics", pt)

    if "legalBasis" in df.columns:
        pl = []
        for _, row in df[["id", "legalBasis"]].explode("legalBasis").iterrows():
            if pd.notnull(row["legalBasis"]) and row["legalBasis"]:
                pl.append({"project_id": row["id"], "legal_basis_code": row["legalBasis"]})
        if pl:
            batch_insert("project_legal_basis", pl)

    if "sci_voc" in df.columns:
        sv = []
        for _, row in df[["id", "sci_voc"]].explode("sci_voc").iterrows():
            if pd.notnull(row["sci_voc"]) and row["sci_voc"]:
                sv.append({"project_id": row["id"], "sci_voc_code": row["sci_voc"]})
        if sv:
            batch_insert("project_sci_voc", sv)

    if "institutions" in df.columns:
        po = []
        for _, row in df[["id", "institutions"]].explode("institutions").iterrows():
            if pd.notnull(row["institutions"]) and row["institutions"]:
                po.append({"project_id": row["id"], "organization_id": row["institutions"]})
        if po:
            batch_insert("project_organizations", po)

def load_topics():
    path = "data/processed/topics_df.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = ["code", "title"]
    cols = [c for c in schema if c in df.columns]
    batch_insert("topics", df[cols].to_dict(orient="records"))

def load_legal_basis():
    path = "data/processed/legal_basis_df.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = ["code", "title", "unique_programme_part"]
    cols = [c for c in schema if c in df.columns]
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
    cols = [c for c in schema if c in df.columns]
    batch_insert("organizations", df[cols].to_dict(orient="records"))

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
    cols = [c for c in schema if c in df.columns]
    batch_insert("deliverables", df[cols].to_dict(orient="records"))

def load_publications():
    path = "data/interim/data_publications.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = [
        "id", "project_id", "title", "is_published_as", "authors", "journal_title",
        "journal_number", "published_year", "published_pages", "issn", "isbn",
        "doi", "collection", "content_update_date"
    ]
    cols = [c for c in schema if c in df.columns]
    batch_insert("publications", df[cols].to_dict(orient="records"))

def load_sci_voc():
    path = "data/processed/sci_voc_df.csv"
    df = safe_load_csv(path)
    if df.empty: return
    schema = ["code", "path", "title", "description"]
    cols = [c for c in schema if c in df.columns]
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
    cols = [c for c in schema if c in df.columns]
    batch_insert("web_links", df[cols].to_dict(orient="records"))

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
