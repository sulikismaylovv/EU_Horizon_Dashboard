import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)

# Add this at the top:
COLUMN_ALIASES = {
    "startdate": "start_date",
    "enddate": "end_date",
    "totalcost": "total_cost",
    "ecmaxcontribution": "ec_max_contribution",
    "ecsignaturedate": "ec_signature_date",
    "frameworkprogramme": "framework_programme",
    "mastercall": "master_call",
    "subcall": "sub_call",
    "fundingscheme": "funding_scheme",
    "contentupdatedate": "content_update_date",
    "grantdoi": "grant_doi",
    "eccontribution_per_year": "ec_contribution_per_year",
    "totalcost_per_year": "total_cost_per_year",
    "legalbasis": "code",
    "topic": "code",
    "uniqueprogrammepart": "unique_programme_part",
    "deliverabletype": "deliverable_type",
    "projectid": "project_id",
    "projectid_x": "project_id",
    "projectid_y": "project_id",
    "eccontribution": "ec_contribution",
    "neteccontribution": "net_ec_contribution",
    "ispublishedas": "is_published_as",
    "journaltitle": "journal_title",
    "journalnumber": "journal_number",
    "publishedyear": "published_year",
    "publishedpages": "published_pages",
    "euroscivoccode": "code",
    "euroscivocpath": "path",
    "euroscivoctitle": "title",
    "euroscivocdescription": "description",
    "availablelanguages": "available_languages",
    "archiveddate": "archived_date",
    "physurl": "phys_url",
    "vatnumber": "vat_number",
    "shortname": "short_name",
    "activitytype": "activity_type",
    "postcode": "post_code",
    "nutscode": "nuts_code",
    "organizationurl": "organization_url",
    "contactform": "contact_form"
}


# Define expected schema for each file
EXPECTED_SCHEMAS = {
    "project_df.csv": [
        "id", "acronym", "status", "title", "start_date", "end_date",
        "total_cost", "ec_max_contribution", "ec_signature_date", "framework_programme",
        "master_call", "sub_call", "funding_scheme", "nature", "objective",
        "content_update_date", "rcn", "grant_doi", "duration_days", "duration_months",
        "duration_years", "n_institutions", "coordinator_name",
        "ec_contribution_per_year", "total_cost_per_year"
    ],
    "organization_df.csv": [
        "id", "name", "short_name", "vat_number", "sme", "activity_type",
        "street", "post_code", "city", "country", "nuts_code", "geolocation",
        "organization_url", "contact_form", "content_update_date"
    ],
    "topics_df.csv": ["code", "title"],
    "legal_basis_df.csv": ["code", "title", "unique_programme_part"],
    "data_deliverables.csv": [
        "id", "project_id", "title", "deliverable_type", "description",
        "url", "collection", "content_update_date"
    ],
    "data_publications.csv": [
        "id", "project_id", "title", "is_published_as", "authors", "journal_title",
        "journal_number", "published_year", "published_pages", "issn", "isbn",
        "doi", "collection", "content_update_date"
    ],
    "sci_voc_df.csv": ["code", "path", "title", "description"],
    "web_items_df.csv": [
        "language", "available_languages", "uri", "title", "type", "source", "represents"
    ],
    "web_link_df.csv": [
        "id", "project_id", "phys_url", "available_languages", "status", "archived_date",
        "type", "source", "represents"
    ]
}

DATA_DIR = "data/processed"
INTERIM_DIR = "data/interim"

def validate_csv(file_name, expected_columns):
    path = os.path.join(DATA_DIR if "processed" in file_name or file_name.startswith("project") else INTERIM_DIR, file_name)
    if not os.path.exists(path):
        logging.warning(f"Missing file: {path}")
        return

    df = pd.read_csv(path, nrows=5)
    df.columns = df.columns.str.strip().str.lower()

    # Apply alias mapping
    df.rename(columns={k.lower(): v for k, v in COLUMN_ALIASES.items()}, inplace=True)


    missing = [col for col in expected_columns if col.lower() not in df.columns]
    extra = [col for col in df.columns if col.lower() not in [c.lower() for c in expected_columns]]

    if missing:
        logging.error(f"❌ Missing columns in {file_name}: {missing}")
    if extra:
        logging.info(f"ℹ️ Extra columns in {file_name}: {extra}")
    if not missing:
        logging.info(f"✅ {file_name} matches expected schema.")

def main():
    for fname, schema in EXPECTED_SCHEMAS.items():
        validate_csv(fname, schema)

if __name__ == "__main__":
    main()
