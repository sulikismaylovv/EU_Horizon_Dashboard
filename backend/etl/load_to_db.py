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
# Silence verbose HTTP logs from PostgREST
logging.getLogger("postgrest").setLevel(logging.WARNING)

# ──────────────────────────────────────────────────────────────────────────────
# Config
DATA_DIR = "data/processed"
BATCH_SIZE = 200

ON_CONFLICT = {
    "projects":              "id",
    "organizations":         "id",
    "topics":                "code",
    "legal_basis":           "code",
    "sci_voc":               "code",
    "project_topics":        "project_id,topic_code",
    "project_legal_basis":   "project_id,legal_basis_code",
    "project_sci_voc":       "project_id,sci_voc_code",
    "project_organizations": "project_id,organization_id",
    "deliverables":          "id",
    "publications":          "id",
    "web_links":             "id",
}


# ──────────────────────────────────────────────────────────────────────────────
def clean_date(val):
    """Normalize datetimes to ISO strings or None."""
    try:
        if pd.isnull(val) or val in (None, "", "nan", "NaT"):
            return None
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isnull(dt):
            dt = date_parse(str(val), fuzzy=True)
        return dt.isoformat()
    except Exception:
        return None


def fix_blanks(df: pd.DataFrame) -> pd.DataFrame:
    # Convert all empty strings to NA
    return df.replace({"": pd.NA})


def fix_floats_and_dates(records, date_fields=None):
    date_fields = date_fields or set()
    for row in records:
        for k, v in row.items():
            if k in date_fields:
                row[k] = clean_date(v)
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
    return records


def batch_upsert(table, records, date_fields=None):
    if not records:
        logging.info(f"{table}: no records to upsert")
        return
    # dedupe by conflict key to avoid "affect row a second time"
    conflict = ON_CONFLICT.get(table)
    if conflict and "," not in conflict:
        key = conflict
        seen = set()
        unique = []
        for r in records:
            val = r.get(key)
            if val in seen:
                continue
            seen.add(val)
            unique.append(r)
        records = unique
    logging.info(f"⏳ {table}: upserting {len(records)} records")
    records = fix_floats_and_dates(records, date_fields)
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        if conflict:
            res = supabase.table(table).upsert(batch, on_conflict=conflict).execute()
        else:
            res = supabase.table(table).insert(batch).execute()
        if getattr(res, "status_code", None) not in (200, 201):
            logging.error(f"{table} batch {i} failed: {res}")


def safe_load_csv(name, **kwargs) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        logging.error(f"Missing CSV: {path}")
        return pd.DataFrame()
    df = pd.read_csv(
        path,
        dtype=str,
        na_values=[""],       # treat empty strings as NA
        keep_default_na=True,
        **kwargs
    )
    return df


def safe_json_load(val):
    if pd.isna(val):
        return []
    try:
        return json.loads(val) if isinstance(val, str) else list(val)
    except Exception:
        return []

# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Core tables

def load_core():
    # Projects
    df = safe_load_csv("projects.csv")
    df = fix_blanks(df)
    df.rename(columns={"startdate":"start_date","enddate":"end_date"}, inplace=True)
    df = df.drop_duplicates(subset=["id"])
    project_cols = [
        "id","acronym","status","title",
        "start_date","end_date","total_cost","ec_max_contribution","ec_signature_date",
        "framework_programme","master_call","sub_call","funding_scheme","nature","objective","content_update_date",
        "rcn","grant_doi",
        "duration_days","duration_months","duration_years",
        "n_institutions","coordinator_name","ec_contribution_per_year","total_cost_per_year"
    ]
    batch_upsert(
        "projects",
        df[project_cols].where(pd.notnull(df), None).to_dict(orient="records"),
        date_fields={"start_date","end_date","ec_signature_date","content_update_date"}
    )

    # Organizations
    df = safe_load_csv("organizations.csv")
    df = fix_blanks(df)
    df["sme"] = df["sme"].astype(str).str.lower().map({"true":True,"false":False}).fillna(False)
    df = df.drop_duplicates(subset=["id"])
    org_cols = [
        "id","name","short_name","vat_number","sme","activity_type",
        "street","post_code","city","country","nuts_code","geolocation",
        "organization_url","contact_form","content_update_date"
    ]
    batch_upsert(
        "organizations",
        df[org_cols].where(pd.notnull(df), None).to_dict(orient="records"),
        date_fields={"content_update_date"}
    )

    # Topics
    df = safe_load_csv("topics.csv")
    df = fix_blanks(df)
    batch_upsert("topics", df[["code","title"]].drop_duplicates("code").to_dict(orient="records"))

    # Legal basis
    df = safe_load_csv("legal_basis.csv")
    df = fix_blanks(df)
    lb = df[["code","title","unique_programme_part"]].drop_duplicates("code").to_dict(orient="records")
    batch_upsert("legal_basis", lb)

    # SciVoc
    df = safe_load_csv("sci_voc.csv")
    df = fix_blanks(df)
    sv = df[["code","path","title","description"]].drop_duplicates("code").to_dict(orient="records")
    batch_upsert("sci_voc", sv)

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Relationships

def load_relationships():
    # Load project_topics.csv
    df = safe_load_csv("project_topics.csv")
    df = fix_blanks(df)
    df = df.drop_duplicates(subset=["project_id", "topic_code"])
    batch_upsert(
        "project_topics",
        df[["project_id", "topic_code"]].where(pd.notnull(df), None).to_dict(orient="records"),
        date_fields=set()  # No date fields in this table
    )
    
    
    # Load project_legal_basis.csv
    df = safe_load_csv("project_legal_basis.csv")
    df = fix_blanks(df)
    df = df.drop_duplicates(subset=["project_id", "legal_basis_code"])
    batch_upsert(
        "project_legal_basis",
        df[["project_id", "legal_basis_code"]].where(pd.notnull(df), None).to_dict(orient="records"),
        date_fields=set()  # No date fields in this table
    )
    
    # Load project_sci_voc.csv
    df = safe_load_csv("project_sci_voc.csv")
    df = fix_blanks(df)
    df = df.drop_duplicates(subset=["project_id", "sci_voc_code"])
    batch_upsert(
        "project_sci_voc",
        df[["project_id", "sci_voc_code"]].where(pd.notnull(df), None).to_dict(orient="records"),
        date_fields=set()  # No date fields in this table
    )
    
    # Load project_organizations.csv
    df = safe_load_csv("project_organizations.csv")
    df = fix_blanks(df)
    df = df.drop_duplicates(subset=["project_id", "organization_id"])
    df["end_of_participation"] = df["end_of_participation"].apply(clean_date)
    batch_upsert(
        "project_organizations",
        df[[
            "project_id", "organization_id", "role", "order_index", "end_of_participation"
        ]].where(pd.notnull(df), None).to_dict(orient="records"),
        date_fields={"end_of_participation"}
    )   

# ──────────────────────────────────────────────────────────────────────────────
# Stage 3: Auxiliaries

def load_aux():
    # Deliverables
    df = safe_load_csv("deliverables.csv"); df = fix_blanks(df)
    df = df.drop_duplicates(subset=["id"])
    # Ensure project_id is numeric, coercing errors to NaN
    df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")

    # Drop any rows where it failed
    bad = df["project_id"].isna()
    if bad.any():
        logging.warning("Skipping deliverables with invalid project_id: %s", df.loc[bad, "id"].tolist())
    df = df.loc[~bad]

    # Now cast to int
    df["project_id"] = df["project_id"].astype(int)

    # Proceed with your upsert
    cols = [
        "id","project_id","title","deliverable_type",
        "description","url","collection","content_update_date"
    ]
    logging.info("Upserting %d deliverables", len(df))
    #batch_upsert(
    #    "deliverables",
    #    df[cols].where(pd.notnull(df), None).to_dict("records"),
    #    date_fields={"content_update_date"}
    #)

    # Publications
    df = safe_load_csv("publications.csv"); df = fix_blanks(df)
    df = df.drop_duplicates(subset=["id"])
    df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
    bad = df["project_id"].isna()
    if bad.any(): logging.warning("Skipping publications with invalid project_id: %s", df.loc[bad, 'id'].tolist())
    df = df.loc[~bad]
    cols = ["id","project_id","title","is_published_as","authors","journal_title","journal_number","published_year","published_pages","issn","isbn","doi","collection","content_update_date"]
    logging.info("Upserting %d publications", len(df))
    #batch_upsert("publications", df[cols].where(pd.notnull(df), None).to_dict("records"), date_fields={"content_update_date"})

    # Web items
    load_web_items()
    # Note: web_items are loaded separately due to their unique structure



    # Web links
    load_web_links()
    # Note: web_links are loaded separately due to their unique structure
    
    
# ──────────────────────────────────────────────────────────────────────────────
# Stage X: Custom tables


def load_web_links():
    print("Loading web_links.csv")
    df = safe_load_csv("web_links.csv")
    df = fix_blanks(df)

    # normalize JSON arrays
    df["available_languages"] = df["available_languages"].apply(safe_json_load)

    # rename to match DDL if needed
    df = df.rename(columns={
        "physUrl": "phys_url",
        "archivedDate": "archived_date"
    })

    # coerce project_id → int, invalid → NaN
    df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
    # coerce represents → int, invalid → NaN
    df["represents"] = pd.to_numeric(df["represents"], errors="coerce")

    # any rows where represents is NaN, fallback to project_id
    mask = df["represents"].isna()
    if mask.any():
        bad_reprs = df.loc[mask, ["id","project_id","represents"]]
        logging.warning(
            "web_links: %d rows had bad represents; falling back to project_id: %s",
            mask.sum(),
            bad_reprs.to_dict("records")
        )
        df.loc[mask, "represents"] = df.loc[mask, "project_id"]

    # Now cast to Python ints or None
    df["project_id"]  = df["project_id"].apply(lambda x: int(x) if pd.notna(x) else None)
    df["represents"]  = df["represents"].apply(lambda x: int(x) if pd.notna(x) else None)

    # pick only the DDL columns
    cols = [
      "id", "project_id", "phys_url", "available_languages",
      "status", "archived_date", "type", "source", "represents"
    ]
    df = df.loc[:, cols]

    records = df.where(pd.notnull(df), None).to_dict("records")
    if not records:
        logging.info("No web_links to insert; skipping.")
        return

    logging.info("Upserting %d web_links", len(records))
    batch_upsert("web_links", records, date_fields={"archived_date"})
    

def load_web_items():
    print("Loading web_items.csv")
    df = safe_load_csv("web_items.csv")
    df = fix_blanks(df)
    df["available_languages"] = df["available_languages"].apply(safe_json_load)

    # coerce project_id → float, then int or fallback
    df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce")
    bad = df[df["project_id"].isna()]
    if not bad.empty:
        logging.warning(
            "web_items: %d rows had invalid project_id; using fallback 101039048: %s",
            len(bad),
            bad[["language","title","type","project_id"]]
               .fillna("nan").to_dict("records")
        )
    df["project_id"] = df["project_id"].apply(
        lambda x: int(x) if pd.notna(x) else 101039048
    )

    # build clean payload, converting any leftover NaN→None
    cols = ["language","available_languages","uri","title","type","source","project_id"]
    records = []
    for _, row in df.iterrows():
        rec = {}
        for c in cols:
            v = row[c]
            # catch numpy NaN or inf
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                rec[c] = None
            else:
                rec[c] = v
        records.append(rec)

    if not records:
        logging.info("No web_items to insert; skipping.")
        return

    logging.info("Inserting %d web_items", len(records))
    resp = supabase.table("web_items").insert(records).execute()
    if getattr(resp, "status_code", None) not in (200,201):
        logging.error("web_items insert failed: %s", resp)
    else:
        logging.info("web_items inserted successfully")

# ──────────────────────────────────────────────────────────────────────────────
def main():
    logging.info("=== Stage 1: Core tables ===")
    #load_core()
    
    logging.info("=== Stage 2: Relationship tables ===")
    #load_relationships()

    logging.info("=== Stage 3: Auxiliary tables ===")
    load_aux()
    
    logging.info("=== Stage X: Adding more custom tables ===")
    # Add any custom tables here, e.g.:
    # load_custom_table("custom_table.csv", "custom_table")

    logging.info("✅ Data load to Supabase complete!")

if __name__ == "__main__":
    main()
