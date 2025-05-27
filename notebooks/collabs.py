import pandas as pd
from itertools import combinations
from tqdm import tqdm  # optional for progress bar


import pandas as pd

import os
import sys
from pathlib import Path


# load functions
project_root = Path.cwd().parent  # assumes you're in /notebooks
sys.path.append(str(project_root))

from backend.etl.ingestion import robust_csv_reader

# get projects and organizations
notebook_dir = os.getcwd()
base_dir = os.path.dirname(notebook_dir)

df_proj = robust_csv_reader(f'{base_dir}/data/processed/project_df.csv', delimiter=',')
df_org = robust_csv_reader(f'{base_dir}/data/processed/organization_df.csv', delimiter=',')

# change keys
columns_renamed = {
    'status': 'status',
    'title': 'title',
    'startDate': 'start_date',
    'endDate': 'end_date',
    'totalCost': 'total_cost',
    'ecMaxContribution': 'ec_max_contribution',
    'ecSignatureDate': 'ec_signature_date',
    'frameworkProgramme': 'framework_programme',
    'masterCall': 'master_call',
    'subCall': 'sub_call',
    'fundingScheme': 'funding_scheme',
    'nature': 'nature',
    'objective': 'objective',
    'contentUpdateDate': 'content_update_date',
    'rcn': 'rcn',
    'grantDoi': 'grant_doi',
    'duration_days': 'duration_days',
    'duration_months': 'duration_months',
    'duration_years': 'duration_years',
    'n_institutions': 'n_institutions',
    'coordinator_name': 'coordinator_name',
    'ecContribution_per_year': 'ec_contribution_per_year',
    'totalCost_per_year': 'total_cost_per_year',
    'field_class': 'field_class',
    'field': 'field',
    'subfield': 'sub_field',
    'niche': 'niche'
}
df_proj = df_proj.rename(columns=columns_renamed)

org_column_renaming = {
    'projectID': 'id',
    'projectAcronym': 'project_acronym',
    'organisationID': 'organisation_id',
    'vatNumber': 'vat_number',
    'name': 'name',
    'shortName': 'short_name',
    'SME': 'sme',
    'activityType': 'activity_type',
    'street': 'street',
    'postCode': 'post_code',
    'city': 'city',
    'country': 'country',
    'nutsCode': 'nuts_code',
    'geolocation': 'geolocation',
    'organizationURL': 'organization_url',
    'contactForm': 'contact_form',
    'contentUpdateDate': 'content_update_date',
    'rcn': 'rcn',
    'order': 'order',
    'role': 'role',
    'ecContribution': 'ec_contribution',
    'netEcContribution': 'net_ec_contribution',
    'totalCost': 'total_cost',
    'endOfParticipation': 'end_of_participation',
    'active': 'active'
}

df_org = df_org.rename(columns=org_column_renaming)

city_locs = df_org['geolocation'].str.split(',', expand=True)

df_org['latitude'] = pd.to_numeric(city_locs[0], errors='coerce')
df_org['longitude'] = pd.to_numeric(city_locs[1], errors='coerce')

# Filter organizations with coordinates
orgs_with_coords = df_org.dropna(subset=['latitude', 'longitude'])

# Container for collaboration edges
collaborations = []

# Group organizations by project ID
project_groups = orgs_with_coords.groupby('id')

for pid, group in tqdm(project_groups, desc="Processing projects"):
    coords = group[['organisation_id', 'latitude', 'longitude', 'name']].values
    if len(coords) < 2:
        continue  # Skip projects with less than 2 valid orgs
    
    # Get project metadata
    project_row = df_proj[df_proj['id'] == pid]
    acronym = project_row['acronym'].values[0] if not project_row.empty else ''
    
    # Create all unique pairs of collaborators (undirected)
    for (id1, lat1, lon1, name1), (id2, lat2, lon2, name2) in combinations(coords, 2):
        collaborations.append({
            'project_id': pid,
            'project_acronym': acronym,
            'org1_id': id1,
            'org1_name': name1,
            'org1_lat': lat1,
            'org1_lon': lon1,
            'org2_id': id2,
            'org2_name': name2,
            'org2_lat': lat2,
            'org2_lon': lon2
        })

# Create a DataFrame and save to CSV
collab_df = pd.DataFrame(collaborations)
collab_df.to_csv(f'{base_dir}/data/processed/project_collaborations.csv', index=False)

print(f"Saved {len(collab_df)} collaboration edges.")
