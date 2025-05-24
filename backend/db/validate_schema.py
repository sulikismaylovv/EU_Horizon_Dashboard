import re
import os
import sys
import logging
import pandas as pd

# 1️⃣ Setup logging to UTF-8 so you can keep any unicode you like
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
if hasattr(handler.stream, "reconfigure"):
    handler.stream.reconfigure(encoding="utf-8")
logging.getLogger().addHandler(handler)
logging.basicConfig(level=logging.INFO, handlers=[handler])

# 2️⃣ Paths
DATA_DIR       = "data/processed"
DDL_SQL_PATH   = "supabase/migrations/20250523_create_core_tables.sql"

# 3️⃣ Expected output schemas matching your exported CSVs
REQUIRED_SCHEMAS = {
    "projects.csv": [
        "id", "acronym", "status", "title",
        "start_date", "end_date", "total_cost", "ec_max_contribution", "ec_signature_date",
        "frameworkProgramme", "masterCall", "subCall", "fundingScheme", "nature", "objective", "content_update_date",
        "rcn", "grant_doi",
        "duration_days", "duration_months", "duration_years",
        "n_institutions", "coordinator_name", "ecContribution_per_year", "totalCost_per_year"
    ],
    "topics.csv": ["code", "title"],
    "project_topics.csv": ["project_id", "topic_code"],
    "legal_basis.csv": ["code", "title", "unique_programme_part"],
    "project_legal_basis.csv": ["project_id", "legal_basis_code"],
    "organizations.csv": [
        "id", "name", "short_name", "vat_number", "sme", "activity_type",
        "street", "post_code", "city", "country", "nuts_code", "geolocation",
        "organization_url", "contact_form", "content_update_date"
    ],
    "project_organizations.csv": [
        "project_id", "organization_id", "role", "order_index", "ec_contribution",
        "net_ec_contribution", "total_cost", "end_of_participation", "active"
    ],
    "deliverables.csv": [
        "id", "project_id", "title", "deliverable_type", "description", "url", "collection", "content_update_date"
    ],
    "publications.csv": [
        "id", "project_id", "title", "is_published_as", "authors",
        "journal_title", "journal_number", "published_year",
        "published_pages", "issn", "isbn", "doi", "collection", "content_update_date"
    ],
    "sci_voc.csv": ["code", "path", "title", "description"],
    "project_sci_voc.csv": ["project_id", "sci_voc_code"],
    "web_items.csv": ["language", "available_languages", "uri", "title", "type", "source", "project_id"],
    "web_links.csv": [
        "id", "project_id", "phys_url", "available_languages", "status", "archived_date", "type", "source", "represents"
    ]
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def load_sql_ddl(path):
    """Read the SQL file and return a dict table->list of columns."""
    text = open(path, encoding="utf-8").read()
    schemas = {}
    for tbl_match in re.finditer(
        r"CREATE TABLE public\.(\w+)\s*\((.*?)\);", text, re.DOTALL | re.IGNORECASE
    ):
        table = tbl_match.group(1)
        cols_block = tbl_match.group(2)
        cols = []
        for line in cols_block.splitlines():
            m = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)\s+", line.strip())
            if m:
                cols.append(m.group(1))
        schemas[table] = cols
    return schemas


def validate_csv(file_name, expected_cols, sql_cols):
    """
    1) Load CSV header
    2) Compare to expected_cols
    3) Compare to SQL DDL
    """
    src_dir = DATA_DIR
    path = os.path.join(src_dir, file_name)
    if not os.path.exists(path):
        logging.error(f"CSV missing: {path}")
        return False

    df = pd.read_csv(path, nrows=0)
    cols = [c.strip() for c in df.columns]

    missing = [c for c in expected_cols if c not in cols]
    extra   = [c for c in cols if c not in expected_cols]
    if missing:
        logging.error(f"{file_name}: missing expected columns: {missing}")
    if extra:
        logging.warning(f"{file_name}: extra columns beyond expected: {extra}")
    else:
        logging.info(f"{file_name}: local schema OK")

    table = file_name.replace('.csv', '')
    sql_columns = sql_cols.get(table, [])
    sql_missing = [c for c in expected_cols if c not in sql_columns]
    csv_extra   = [c for c in cols if c not in sql_columns]
    if sql_missing:
        logging.error(f"{file_name}: expected columns not in database DDL: {sql_missing}")
    if csv_extra:
        logging.warning(f"{file_name}: CSV has columns not in DDL ({table}): {csv_extra}")

    return not (missing or sql_missing)


def main():
    sql_schemas = load_sql_ddl(DDL_SQL_PATH)
    all_ok = True

    logging.info("Validating CSV schemas against expected lists and DDL...")
    for fname, expected in REQUIRED_SCHEMAS.items():
        ok = validate_csv(fname, expected, sql_schemas)
        all_ok = all_ok and ok

    if all_ok:
        logging.info("All CSV schemas align with expected columns and DDL.")
    else:
        logging.error("Schema validation failed. Fix errors before loading.")
        sys.exit(1)

if __name__ == "__main__":
    main()