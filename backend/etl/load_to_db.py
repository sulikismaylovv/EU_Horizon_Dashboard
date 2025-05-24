import os
import json
import logging
import math
from dateutil.parser import parse as date_parse

import pandas as pd
from backend.db.supabase_client import supabase

# ──────────────────────────────────────────────────────────────────────────────
# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("load_warnings.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ──────────────────────────────────────────────────────────────────────────────
# Config
DATA_DIR = "data/processed"
BATCH_SIZE = 200

ON_CONFLICT = {
    "projects":                "id",
    "organizations":           "id",
    "topics":                  "code",
    "legal_basis":             "code",
    "sci_voc":                 "code",
    "project_topics":          "project_id,topic_code",
    "project_legal_basis":     "project_id,legal_basis_code",
    "project_sci_voc":         "project_id,sci_voc_code",
    "project_organizations":   "project_id,organization_id",
    "deliverables":            "id",
    "publications":            "id",
    "web_items":               "id",
    "web_links":               "id",
}

# map any remaining camelCase → snake_case
COLUMN_ALIASES = {
    "startdate":                "start_date",
    "enddate":                  "end_date",
    "ecsignaturedate":          "ec_signature_date",
    "contentupdatedate":        "content_update_date",
    "grantdoi":                 "grant_doi",
    "ecmaxcontribution":        "ec_max_contribution",
    "totalcost":                "total_cost",
    "eccontribution_per_year":  "ec_contribution_per_year",
    "totalcost_per_year":       "total_cost_per_year",
    "frameworkprogramme":       "framework_programme",
    "mastercall":               "master_call",
    "subcall":                  "sub_call",
    "fundingscheme":            "funding_scheme",
    "uniqueprogrammepart":      "unique_programme_part",
    "deliverabletype":          "deliverable_type",
    "ispublishedas":            "is_published_as",
    "journaltitle":             "journal_title",
    "journalnumber":            "journal_number",
    "publishedyear":            "published_year",
    "publishedpages":           "published_pages",
    "physurl":                  "phys_url",
    "archiveddate":             "archived_date",
    "availablelanguages":       "available_languages",
    "organisationid":           "organization_id",
    "projectid":                "project_id",
    "publicationid":            "id",            # in publications.csv
    "deliverableid":            "id",            # in deliverables.csv
}

# ──────────────────────────────────────────────────────────────────────────────
def clean_date(val):
    """Normalize datetimes to ISO strings or None."""
    try:
        if pd.isnull(val) or val in ("", "nan", "NaT", None):
            return None
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isnull(dt):
            dt = date_parse(str(val), fuzzy=True)
        return dt.isoformat()
    except Exception:
        return None

def fix_floats_and_dates(records, date_fields=None):
    date_fields = date_fields or set()
    for row in records:
        for k, v in list(row.items()):
            if k in date_fields:
                row[k] = clean_date(v)
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
    return records

def batch_upsert(table, records, date_fields=None):
    if not records:
        logging.info(f"No records to upsert for {table}")
        return
    conflict = ON_CONFLICT.get(table)
    records = fix_floats_and_dates(records, date_fields)
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        if conflict:
            res = supabase.table(table).upsert(batch, on_conflict=conflict).execute()
        else:
            res = supabase.table(table).insert(batch).execute()
        if getattr(res, "status_code", None) not in (200, 201):
            logging.error(f"{table} batch starting {i} failed: {res}")

def safe_load_csv(name, **kwargs):
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        logging.error(f"Missing CSV: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str, **kwargs)
    # normalize & alias columns
    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={k: v for k, v in COLUMN_ALIASES.items()}, inplace=True)
    return df

def safe_json_load(val):
    if pd.isnull(val):
        return []
    try:
        if isinstance(val, str):
            return json.loads(val)
        return val if isinstance(val, list) else [val]
    except Exception:
        return []

# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Core tables
def load_core():
    # Projects
    df = safe_load_csv("projects.csv", keep_default_na=False)
    df = df.rename(columns={
        # ensure start/end are snake_case
        "startdate": "start_date",
        "enddate":   "end_date",
    })
    cols = [
        "id","acronym","status","title",
        "start_date","end_date","total_cost","ec_max_contribution","ec_signature_date",
        "framework_programme","master_call","sub_call","funding_scheme","nature","objective","content_update_date",
        "rcn","grant_doi",
        "duration_days","duration_months","duration_years",
        "n_institutions","coordinator_name","ec_contribution_per_year","total_cost_per_year"
    ]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert("projects", df.to_dict("records"), date_fields={"start_date","end_date","ec_signature_date","content_update_date"})

    # Organizations
    df = safe_load_csv("organizations.csv", keep_default_na=False)
    df["sme"] = df["sme"].astype(str).str.lower().map({"true":True,"false":False}).fillna(False)
    cols = [
        "id","name","short_name","vat_number","sme","activity_type",
        "street","post_code","city","country","nuts_code","geolocation",
        "organization_url","contact_form","content_update_date"
    ]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert("organizations", df.to_dict("records"), date_fields={"content_update_date"})

    # Topics
    df = safe_load_csv("topics.csv")
    batch_upsert("topics", df[["code","title"]].drop_duplicates("code").to_dict("records"))

    # Legal basis
    df = safe_load_csv("legal_basis.csv")
    batch_upsert(
        "legal_basis",
        df[["code","title","unique_programme_part"]].drop_duplicates("code").to_dict("records")
    )

    # SciVoc
    df = safe_load_csv("sci_voc.csv")
    batch_upsert(
        "sci_voc",
        df[["code","path","title","description"]].drop_duplicates("code").to_dict("records")
    )

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Relationships
def load_relationships():
    # re-read projects to explode lists
    proj = safe_load_csv("projects.csv")
    # project_topics
    recs = []
    for _, r in proj.iterrows():
        for t in safe_json_load(r.get("topics")):
            recs.append({"project_id": r["id"], "topic_code": t})
    batch_upsert("project_topics", recs)

    # project_legal_basis
    recs = []
    for _, r in proj.iterrows():
        for lb in safe_json_load(r.get("legal_basis")):
            recs.append({"project_id": r["id"], "legal_basis_code": lb})
    batch_upsert("project_legal_basis", recs)

    # project_sci_voc
    recs = []
    for _, r in proj.iterrows():
        for sv in safe_json_load(r.get("sci_voc")):
            recs.append({"project_id": r["id"], "sci_voc_code": sv})
    batch_upsert("project_sci_voc", recs)

    # project_organizations
    recs = []
    for _, r in proj.iterrows():
        for org in safe_json_load(r.get("institutions")):
            recs.append({"project_id": r["id"], "organization_id": org})
    batch_upsert("project_organizations", recs)

# ──────────────────────────────────────────────────────────────────────────────
# Stage 3: Auxiliaries
def load_aux():
    # Deliverables
    df = safe_load_csv("deliverables.csv", keep_default_na=False)
    cols = ["id","project_id","title","deliverable_type","description","url","collection","content_update_date"]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert("deliverables", df.to_dict("records"), date_fields={"content_update_date"})

    # Publications
    df = safe_load_csv("publications.csv", keep_default_na=False)
    cols = ["id","project_id","title","is_published_as","authors","journal_title","journal_number",
            "published_year","published_pages","issn","isbn","doi","collection","content_update_date"]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert("publications", df.to_dict("records"), date_fields={"content_update_date"})

    # Web items (represents → project_id in CSV)
    df = safe_load_csv("web_items.csv", keep_default_na=False)
    df["available_languages"] = df["available_languages"].apply(safe_json_load)
    cols = ["language","available_languages","uri","title","type","source","represents"]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert("web_items", df.to_dict("records"))

    # Web links
    df = safe_load_csv("web_links.csv", keep_default_na=False)
    df["available_languages"] = df["available_languages"].apply(safe_json_load)
    cols = ["id","project_id","phys_url","available_languages","status","archived_date","type","source","represents"]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert("web_links", df.to_dict("records"), date_fields={"archived_date"})

# ──────────────────────────────────────────────────────────────────────────────
def main():
    logging.info("=== Stage 1: Core tables ===")
    load_core()

    logging.info("=== Stage 2: Relationship tables ===")
    load_relationships()

    logging.info("=== Stage 3: Auxiliary tables ===")
    load_aux()

    logging.info("✅ Data load to Supabase complete!")

if __name__ == "__main__":
    main()