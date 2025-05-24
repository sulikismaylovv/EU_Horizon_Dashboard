import os
import json
import logging
import math
from dateutil.parser import parse as date_parse
import pandas as pd
from backend.db.supabase_client import supabase

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[
        logging.FileHandler("load_warnings.log"),
        logging.StreamHandler()
    ]
)

# -- Configuration --
DATA_DIR = "data/processed"

# Upsert batch sizes
BATCH_SIZE = 200

# Conflict keys for upsert operations
ON_CONFLICT = {
    "projects": "id",
    "organizations": "id",
    "topics": "code",
    "legal_basis": "code",
    "sci_voc": "code",
    # join tables:
    "project_topics": "project_id,topic_code",
    "project_legal_basis": "project_id,legal_basis_code",
    "project_sci_voc": "project_id,sci_voc_code",
    "project_organizations": "project_id,organization_id",
    # auxiliary:
    "deliverables": "id",
    "publications": "id",
    "web_items": "id",
    "web_links": "id"
}

# Column renaming to match DB schema
COLUMN_ALIASES = {
    "startdate": "start_date", "enddate": "end_date", "ecsignaturedate": "ec_signature_date",
    "contentupdatedate": "content_update_date", "grantdoi": "grant_doi",
    "ecmaxcontribution": "ec_max_contribution", "totalcost": "total_cost",
    "eccontribution_per_year": "ec_contribution_per_year", "totalcost_per_year": "total_cost_per_year",
    "frameworkprogramme": "framework_programme", "mastercall": "master_call",
    "subcall": "sub_call", "fundingscheme": "funding_scheme", "legalbasis": "legal_basis",
    "topic": "topics", "uniqueprogrammepart": "unique_programme_part", "deliverabletype": "deliverable_type",
    "projectid": "project_id", "physurl": "phys_url", "archiveddate": "archived_date",
    "availablelanguages": "available_languages", "rcn": "rcn"
}

# --- Helper Functions ---
def clean_date(val):
    try:
        if pd.isnull(val) or val in ("NaT", "nan", "None", "", None):
            return None
        dt = pd.to_datetime(val, errors='coerce')
        return dt.isoformat() if pd.notnull(dt) else None
    except Exception:
        try:
            return date_parse(str(val), fuzzy=True).isoformat()
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
            elif hasattr(v, 'isoformat'):
                try:
                    row[k] = v.isoformat()
                except Exception:
                    row[k] = None
    return records


def batch_upsert(table, records):
    if not records:
        logging.info(f"No records to upsert for {table}")
        return
    conflict_key = ON_CONFLICT.get(table)
    records = fix_floats_and_dates(records)
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        if conflict_key:
            resp = supabase.table(table).upsert(batch, on_conflict=conflict_key).execute()
        else:
            resp = supabase.table(table).insert(batch).execute()
        if getattr(resp, 'status_code', None) not in (200, 201):
            logging.error(f"Failed {table} batch starting {i}: {resp}")


def safe_load_csv(path, **kwargs):
    if not os.path.exists(path):
        logging.error(f"CSV not found: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str, **kwargs)
    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={k.lower(): v for k, v in COLUMN_ALIASES.items()}, inplace=True)
    return df


def safe_json_load(val):
    try:
        if isinstance(val, str) and (val.startswith('[') or val.startswith('{')):
            return json.loads(val)
        if pd.isnull(val):
            return []
        return val if isinstance(val, list) else [val]
    except Exception:
        return []

# --- Stage 1: Load Core Tables ---
def load_core():
    # Projects
    df = safe_load_csv(os.path.join(DATA_DIR, 'project_df.csv'), keep_default_na=False)
    cols = [
        'id','acronym','status','title','start_date','end_date','total_cost',
        'ec_max_contribution','ec_signature_date','framework_programme','master_call',
        'sub_call','funding_scheme','nature','objective','content_update_date',
        'rcn','grant_doi','duration_days','duration_months','duration_years',
        'n_institutions','coordinator_name','ec_contribution_per_year','total_cost_per_year'
    ]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert('projects', df.to_dict(orient='records'))

    # Organizations
    df = safe_load_csv(os.path.join(DATA_DIR, 'organization_df.csv'), keep_default_na=False)
    df['sme'] = df.get('sme', 'false').astype(str).str.lower().map({'true': True, 'false': False}).fillna(False)
    cols = [
        'id','name','short_name','vat_number','sme','activity_type',
        'street','post_code','city','country','nuts_code','geolocation',
        'organization_url','contact_form','content_update_date'
    ]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert('organizations', df.to_dict(orient='records'))

    # Topics
    df = safe_load_csv(os.path.join(DATA_DIR, 'topics_df.csv'), keep_default_na=False)
    batch_upsert('topics', df[['code','title']].drop_duplicates('code').to_dict(orient='records'))

    # Legal basis
    df = safe_load_csv(os.path.join(DATA_DIR, 'legal_basis_df.csv'), keep_default_na=False)
    batch_upsert('legal_basis', df[['code','title','unique_programme_part']].drop_duplicates('code').to_dict(orient='records'))

    # Scientific vocab
    df = safe_load_csv(os.path.join(DATA_DIR, 'sci_voc_df.csv'), keep_default_na=False)
    batch_upsert('sci_voc', df[['code','path','title','description']].drop_duplicates('code').to_dict(orient='records'))

# --- Stage 2: Load Relationship Tables ---
def load_relationships():
    # project_topics
    projects = safe_load_csv(os.path.join(DATA_DIR, 'project_df.csv'), keep_default_na=False)
    recs = []
    for _, r in projects.iterrows():
        for t in safe_json_load(r.get('topics')):
            recs.append({'project_id': r['id'], 'topic_code': t})
    batch_upsert('project_topics', recs)

    # project_legal_basis
    recs = []
    for _, r in projects.iterrows():
        for lb in safe_json_load(r.get('legal_basis')):
            recs.append({'project_id': r['id'], 'legal_basis_code': lb})
    batch_upsert('project_legal_basis', recs)

    # project_sci_voc
    recs = []
    for _, r in projects.iterrows():
        for sv in safe_json_load(r.get('sci_voc')):
            recs.append({'project_id': r['id'], 'sci_voc_code': sv})
    batch_upsert('project_sci_voc', recs)

    # project_organizations
    recs = []
    for _, r in projects.iterrows():
        for org in safe_json_load(r.get('institutions')):
            recs.append({'project_id': r['id'], 'organization_id': org})
    batch_upsert('project_organizations', recs)

# --- Stage 3: Load Auxiliary Tables ---
def load_aux():
    # Deliverables
    df = safe_load_csv(os.path.join(DATA_DIR, 'data_deliverables.csv'), keep_default_na=False)
    cols = ['id','project_id','title','deliverable_type','description','url','collection','content_update_date']
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert('deliverables', df.to_dict(orient='records'))

    # Publications
    df = safe_load_csv(os.path.join(DATA_DIR, 'data_publications.csv'), keep_default_na=False)
    cols = [
        'id','project_id','title','is_published_as','authors','journal_title',
        'journal_number','published_year','published_pages','issn','isbn',
        'doi','collection','content_update_date'
    ]
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert('publications', df.to_dict(orient='records'))

    # Web items
    df = safe_load_csv(os.path.join(DATA_DIR, 'web_items_df.csv'), keep_default_na=False)
    df['available_languages'] = df['available_languages'].apply(safe_json_load)
    cols = ['language','available_languages','uri','title','type','source','represents']
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert('web_items', df.to_dict(orient='records'))

    # Web links
    df = safe_load_csv(os.path.join(DATA_DIR, 'web_link_df.csv'), keep_default_na=False)
    df['available_languages'] = df['available_languages'].apply(safe_json_load)
    cols = ['id','project_id','phys_url','available_languages','status','archived_date','type','source','represents']
    df = df[cols].where(pd.notnull(df), None)
    batch_upsert('web_links', df.to_dict(orient='records'))

# --- Main Execution ---
def main():
    logging.info("=== Stage 1: Core tables ===")
    load_core()
    logging.info("=== Stage 2: Relationship tables ===")
    load_relationships()
    logging.info("=== Stage 3: Auxiliary tables ===")
    load_aux()
    logging.info("✅ Data migration complete!")

if __name__ == "__main__":
    main()