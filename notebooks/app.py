import dash
from dash import dcc, html, Input, Output
import plotly.express as px

import numpy as np
import pandas as pd
import plotly.express as px
import os
import sys
from pathlib import Path
import pycountry
import itertools

from dash import dcc, html, Input, Output
from dash import dash_table
import networkx as nx
import plotly.graph_objects as go

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

import wbdata
import pandas as pd
import pycountry
from datetime import datetime

# Fetch population data from World Bank for 2020
data_date = datetime(2020, 1, 1)
indicators = {'SP.POP.TOTL': 'Population'}

df = wbdata.get_dataframe(indicators, date=data_date).reset_index()

# Function to get ISO-3 country code using pycountry
def get_iso3(country_name):
    try:
        return pycountry.countries.lookup(country_name).alpha_3
    except LookupError:
        return None

# Apply ISO-3 code lookup
df['ISO3'] = df['country'].apply(get_iso3)

# Drop rows where ISO3 code couldn't be found (optional)
df = df.dropna(subset=['ISO3'])

# Sort by population descending
df = df.sort_values(by='Population', ascending=False).reset_index(drop=True)
country_populations = df.set_index('ISO3')['Population'].to_dict()

# define country centroids
country_centroids_iso3 = {
    'MDG': (-18.766947, 46.869107), 'SEN': (14.497401, -14.452362), 'ETH': (9.145, 40.489673),
    'GBR': (55.378051, -3.435973), 'CHE': (46.818188, 8.227512), 'FRA': (46.603354, 1.888334),
    'AUS': (-25.274398, 133.775136), 'FIN': (61.92411, 25.748151), 'DNK': (56.26392, 9.501785),
    'ESP': (40.463667, -3.74922), 'SVN': (46.151241, 14.995463), 'LTU': (55.169438, 23.881275),
    'POL': (51.919438, 19.145136), 'NLD': (52.132633, 5.291266), 'PRT': (39.399872, -8.224454),
    'BEL': (50.503887, 4.469936), 'DEU': (51.165691, 10.451526), 'USA': (37.09024, -95.712891),
    'NOR': (60.472024, 8.468946), 'TUR': (38.963745, 35.243322), 'ZAF': (-30.559482, 22.937506),
    'ZMB': (-13.133897, 27.849332), 'ZWE': (-19.015438, 29.154857), 'CIV': (7.539989, -5.54708),
    'SVK': (48.669026, 19.699024), 'BGR': (42.733883, 25.48583), 'ROU': (45.943161, 24.96676),
    'GRC': (39.074208, 21.824312), 'ISR': (31.046051, 34.851612), 'ITA': (41.87194, 12.56738),
    'EST': (58.595272, 25.013607), 'IRL': (53.41291, -8.24389), 'HUN': (47.162494, 19.503304),
    'CZE': (49.817492, 15.472962), 'AUT': (47.516231, 14.550072), 'LVA': (56.879635, 24.603189),
    'UKR': (48.379433, 31.16558), 'GIN': (9.945587, -9.696645), 'MLI': (17.570692, -3.996166),
    'SWE': (60.128161, 18.643501), 'BWA': (-22.328474, 24.684866), 'MOZ': (-18.665695, 35.529562),
    'LSO': (-29.609988, 28.233608), 'SWZ': (-26.522503, 31.465866), 'BFA': (12.238333, -1.561593),
    'GHA': (7.946527, -1.023194), 'CYP': (35.126413, 33.429859), 'MLT': (35.937496, 14.375416),
    'CMR': (7.369722, 12.354722), 'LUX': (49.815273, 6.129583), 'NGA': (9.081999, 8.675277),
    'TZA': (-6.369028, 34.888822), 'MWI': (-13.254308, 34.301525), 'UGA': (1.373333, 32.290275),
    'KEN': (-0.023559, 37.906193), 'CHN': (35.86166, 104.195397), 'IND': (20.593684, 78.96288),
    'KOR': (35.907757, 127.766922), 'SRB': (44.016521, 21.005859), 'EGY': (26.820553, 30.802498),
    'ARG': (-38.416097, -63.616672), 'HRV': (45.1, 15.2), 'ARM': (40.069099, 45.038189),
    'BRA': (-14.235004, -51.92528), 'CPV': (16.5388, -23.0418), 'CAN': (56.130366, -106.346771),
    'TUN': (33.886917, 9.537499), 'AGO': (-11.202692, 17.873887), 'STP': (0.18636, 6.613081),
    'COL': (4.570868, -74.297333), 'BTN': (27.514162, 90.433601), 'PRY': (-23.442503, -58.443832),
    'CAF': (6.611111, 20.939444), 'DZA': (28.033886, 1.659626), 'GNQ': (1.650801, 10.267895),
    'LKA': (7.873054, 80.771797), 'CHL': (-35.675147, -71.542969), 'ALB': (41.153332, 20.168331),
    'ISL': (64.963051, -19.020835), 'COD': (-4.038333, 21.758664), 'BDI': (-3.373056, 29.918886),
    'MEX': (23.634501, -102.552784), 'MNE': (42.708678, 19.37439), 'MNG': (46.862496, 103.846656),
    'THA': (15.870032, 100.992541), 'KAZ': (48.019573, 66.923684), 'JPN': (36.204824, 138.252924),
    'VAT': (41.902916, 12.453389), 'NZL': (-40.900557, 174.885971), 'ECU': (-1.831239, -78.183406),
    'MDA': (47.411631, 28.369885), 'UZB': (41.377491, 64.585262), 'AZE': (40.143105, 47.576927),
    'SGP': (1.352083, 103.819836), 'PAK': (30.375321, 69.345116), 'TWN': (23.69781, 120.960515),
    'GUM': (13.444304, 144.793731), 'CRI': (9.748917, -83.753428), 'PER': (-9.189967, -75.015152),
    'LBN': (33.854721, 35.862285), 'BIH': (43.915886, 17.679076), 'MAR': (31.791702, -7.09262),
    'VNM': (14.058324, 108.277199), 'MKD': (41.608635, 21.745275), 'BEN': (9.30769, 2.315834),
    'GAB': (-0.803689, 11.609444), 'MYS': (4.210484, 101.975766), 'XKX': (42.602636, 20.902977),
    'PSE': (31.952162, 35.233154), 'PHL': (12.879721, 121.774017), 'SAU': (23.885942, 45.079162),
    'RWA': (-1.940278, 29.873888), 'IDN': (-0.789275, 113.921327), 'FRO': (61.892635, -6.911806),
    'CUB': (21.521757, -77.781167), 'KGZ': (41.20438, 74.766098), 'BGD': (23.684994, 90.356331),
    'PYF': (-17.679742, -149.406843), 'LBR': (6.428055, -9.429499), 'SLE': (8.460555, -11.779889),
    'VEN': (6.42375, -66.58973), 'GEO': (42.315407, 43.356892), 'JOR': (30.585164, 36.238414),
    'FJI': (-17.713371, 178.065032), 'URY': (-32.522779, -55.765835), 'COG': (-0.228021, 15.827659),
    'AFG': (33.93911, 67.709953), 'IRQ': (33.223191, 43.679291), 'HKG': (22.396428, 114.109497),
    'TJK': (38.861034, 71.276093), 'TKM': (38.969719, 59.556278), 'BOL': (-16.290154, -63.588653),
    'MDV': (3.202778, 73.22068), 'IMN': (54.236107, -4.548056), 'BRB': (13.193887, -59.543198),
    'BHR': (25.930414, 50.637772), 'GRL': (71.706936, -42.604303), 'GNQ': (1.650801, 10.267895),
    'DMA': (15.415, -61.371), 'MHL': (7.1315, 171.1845),
}
# convert some data
from backend.etl.cleaning import clean_date_column
df_proj['start_year'] = clean_date_column(df_proj['start_date']).dt.year

# get country-project info
proj_country = df_org[['id', 'country']]

# add contribution per project
proj_country = proj_country.merge(
    df_proj[['id', 'ec_max_contribution']],
    on='id',
    how='left'
)

# Calculate total contribution per country (sum over all projects related to that country)
country_data = proj_country.groupby('country')['ec_max_contribution'].sum().reset_index()

# Rename column
country_data = country_data.rename(columns={'ec_max_contribution': 'total_contribution_country'})

# Merge back into proj_country on 'country'
country_data = country_data.merge(proj_country, on='country', how='left')

# remove redundant rows
country_data = country_data[['country', 'total_contribution_country']].drop_duplicates()


# STEP 1: Project-country combinations
proj_country = df_org[['id', 'country']].drop_duplicates()

# STEP 2: Merge EC max contribution per project into proj_country
proj_country = proj_country.merge(
    df_proj[['id', 'ec_max_contribution']],
    on='id',
    how='left'
)

# STEP 3: Ensure ec_max_contribution is numeric and handle missing values
proj_country['ec_max_contribution'] = pd.to_numeric(
    proj_country['ec_max_contribution'], errors='coerce'
).fillna(0.0)

# STEP 4: Total contribution per country (summing contributions of all projects linked to that country)
total_contrib_per_country = proj_country.groupby('country')['ec_max_contribution'].sum().reset_index()
total_contrib_per_country.rename(columns={'ec_max_contribution': 'total_contribution'}, inplace=True)

# STEP 5: Count number of unique projects per country
project_count_per_country = proj_country.groupby('country')['id'].nunique().reset_index()
project_count_per_country.rename(columns={'id': 'project_count'}, inplace=True)

# STEP 6: Merge both into one country-specific DataFrame
country_specific = pd.merge(
    total_contrib_per_country,
    project_count_per_country,
    on='country',
    how='outer'
)

# Count number of unique projects per country
country_counts = proj_country['country'].value_counts().reset_index()
country_counts.columns = ['country', 'project_count']

country_specific['total_contribution'] = pd.to_numeric(
    country_specific['total_contribution'], errors='coerce').fillna(0.0)
country_specific['project_count'] = pd.to_numeric(
    country_specific['project_count'], errors='coerce').fillna(0.0)


# convert to iso_alpha_3
def iso2_to_iso3(iso2):
    try:
        return pycountry.countries.get(alpha_2=iso2).alpha_3
    except:
        return None
# rescale country_specific DataFrame to include ISO alpha-3 codes
country_specific['iso_alpha_3'] = country_specific['country'].apply(iso2_to_iso3)


# Compute € per 100k inhabitants (from the dictionary)
def compute_euro_per_100k(row):
    pop = country_populations.get(row['iso_alpha_3'])
    if pop is None or pop == 0:
        return None
    return row['total_contribution'] / pop * 100_000

country_specific['€/100k_inhabitants'] = country_specific.apply(compute_euro_per_100k, axis=1)
country_specific['€/100k_inhabitants'] = country_specific['€/100k_inhabitants'].fillna(0.0)

country_specific['lat'] = country_specific['iso_alpha_3'].map(lambda c: country_centroids_iso3.get(c, (None, None))[0])
country_specific['lon'] = country_specific['iso_alpha_3'].map(lambda c: country_centroids_iso3.get(c, (None, None))[1])

city_locs = df_org['geolocation'].str.split(',', expand=True)

df_org['latitude'] = pd.to_numeric(city_locs[0], errors='coerce')
df_org['longitude'] = pd.to_numeric(city_locs[1], errors='coerce')

# Ensure numeric contribution
df_proj['ec_max_contribution'] = pd.to_numeric(df_proj['ec_max_contribution'], errors='coerce')

# STEP 1 — Bubble chart data
country_summary = (
    df_org
    .merge(df_proj, on='id')
    .groupby('country')
    .agg(
        total_contribution=('ec_max_contribution', 'sum'),
        project_count=('id', 'count')
    )
    .reset_index()
)

# change to ISO 3 counrty codes
country_summary['iso_alpha_3'] = country_summary['country'].apply(iso2_to_iso3)

country_summary['latitude'] = country_summary['iso_alpha_3'].map(lambda x: country_centroids_iso3.get(x, (None, None))[0])
country_summary['longitude'] = country_summary['iso_alpha_3'].map(lambda x: country_centroids_iso3.get(x, (None, None))[1])
country_summary['log_contribution'] = np.log10(country_summary['total_contribution'] + 1)

# get all unique items from a list
def extract_unique_from_lists(series):
    # Flatten all lists in the series and get unique values
    all_items = list(itertools.chain.from_iterable(
        x if isinstance(x, list) else [x] if pd.notnull(x) else [] for x in series
    ))
    return sorted(set(all_items))

# If your columns are stored as strings of lists, convert them to lists first
import ast
for col in ['field_class', 'field', 'sub_field', 'niche']:
    df_proj[col] = df_proj[col].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x)

field_class_options = extract_unique_from_lists(df_proj['field_class'])
field_options = extract_unique_from_lists(df_proj['field'])
subfield_options = extract_unique_from_lists(df_proj['sub_field'])
niche_options = extract_unique_from_lists(df_proj['niche'])

# Code for the network plot
def plot_collaboration_network(filtered_org, filtered_projects):
    """
    Plot a collaboration network of organizations for the selected projects.
    Nodes are organizations, edges connect organizations that collaborate in the same project.
    """
    G = nx.Graph()
    # Only use organizations from filtered_org and projects from filtered_projects
    orgs = filtered_org[filtered_org['id'].isin(filtered_projects['id'])]
    for pid, group in orgs.groupby('id'):
        org_names = group['name'].tolist()
        # Add edges between all pairs of organizations in the same project
        for i in range(len(org_names)):
            for j in range(i + 1, len(org_names)):
                G.add_edge(org_names[i], org_names[j])

    if len(G.nodes) == 0:
        # Return an empty figure if no collaborations
        return go.Figure()

    pos = nx.spring_layout(G, seed=42, k=0.5)
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    node_x = []
    node_y = []
    node_text = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    ))
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        marker=dict(size=10, color='blue'),
        text=node_text,
        textposition="top center",
        hoverinfo='text'
    ))
    fig.update_layout(
        title="Collaboration Network of Selected Projects",
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=20),
        height=800,
        width=1000
    )
    return fig

# add ISO 3 country codes
df_org['iso_alpha_3'] = df_org['country'].apply(iso2_to_iso3)

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H3("Horizon Europe Project Funding Visualization"),

    html.Div([
        # First column
        html.Div([
            html.Label("Select Country:"),
            dcc.Dropdown(
                id='country-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': code, 'value': code} for code in sorted(df_org['iso_alpha_3'].dropna().unique())
                ],
                value='all'
            ),
            html.Label("Select funding scheme:"),
            dcc.Dropdown(
                id='funding-scheme-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': fs, 'value': fs} for fs in sorted(df_proj['funding_scheme'].dropna().unique())
                ],
                value='all',
                placeholder="Select funding scheme",
                style={'margin': '10px 0'}
            ),
            html.Label("Select starting year:"),
            dcc.Dropdown(
                id='year-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': str(y), 'value': y} for y in sorted(df_proj['start_year'].dropna().unique())
                ],
                value='all',
                placeholder="Select starting year",
                style={'margin': '10px 0'}
            )
        ], style={'flex': 1, 'margin': '10px'}),
        # Second column
        html.Div([
            html.Label("Select field class:"),
            dcc.Dropdown(
                id='field-class-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': fc, 'value': fc} for fc in field_class_options
                ],
                value='all',
                multi=True,
                placeholder="Select field class",
                style={'margin': '10px 0'}
            ),
            html.Label("Select field:"),
            dcc.Dropdown(
                id='field-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': f, 'value': f} for f in field_options
                ],
                value='all',
                multi=True,
                placeholder="Select field",
                style={'margin': '10px 0'}
            ),
            html.Label("Select subfield:"),
            dcc.Dropdown(
                id='subfield-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': sf, 'value': sf} for sf in subfield_options
                ],
                value='all',
                multi=True,
                placeholder="Select subfield",
                style={'margin': '10px 0'}
            ),
            html.Label("Select niche:"),
            dcc.Dropdown(
                id='niche-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': n, 'value': n} for n in niche_options
                ],
                value='all',
                multi=True,
                placeholder="Select niche",
                style={'margin': '10px 0'}
            )
        ], style={'flex': 1, 'margin': '10px'}),
        # Third column
        html.Div([
            html.Label("Select activity type:"),
            dcc.Dropdown(
                id='activity-type-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': at, 'value': at} for at in sorted(df_org['activity_type'].dropna().unique())
                ],
                value='all',
                placeholder="Select institution type",
                style={'margin': '10px 0'}
            ),
            html.Label("Select organization role:"),
            dcc.Dropdown(
                id='role-dropdown',
                options=[{'label': 'All', 'value': 'all'}] + [
                    {'label': role, 'value': role} for role in sorted(df_org['role'].dropna().unique())
                ],
                value='all',
                placeholder="Select organization role",
                style={'margin': '10px 0'}
            ),
            html.Label("Rescale total EC contribution to contribution per 100k inhabitants:"),
            dcc.Checklist(
                id='per-capita-toggle',
                options=[{'label': 'Show per 100k inhabitants', 'value': 'per_capita'}],
                value=[],
                style={'margin': '10px 0'}
            ),
            html.Label("Show network of collaborations:"),
            dcc.Checklist(
                id='network-toggle',
                options=[{'label': 'Show collaboration network', 'value': 'show_network'}],
                value=[],
                style={'margin': '10px 0'}
            ),
            html.Label("Show organization pins:"),
            dcc.Checklist(
                id='org-pins-toggle',
                options=[{'label': 'Show organization pins', 'value': 'show_pins'}],
                value=['show_pins'],
                style={'margin': '10px 0'}
            ),
        ], style={'flex': 1, 'margin': '10px'})
    ], style={'display': 'flex', 'flex-direction': 'row', 'justify-content': 'space-between'}),


    
    dcc.Graph(id='map-graph', config={'scrollZoom': True}),
    html.H4("Projects shown on the map:"),
    dash_table.DataTable(
        id='project-table',
        columns=[
            {"name": "Acronym", "id": "project_acronym"},
            {"name": "EC Contribution", "id": "ec_max_contribution", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Title", "id": "title"},
            {"name": "Institutes", "id": "institutes"},
        ],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'left', 'padding': '5px'},
        style_header={'fontWeight': 'bold'},
        page_size=10
    )
])

def filter_by_list_column(df, col, selected):
    if not selected or selected == 'all' or (isinstance(selected, list) and 'all' in selected):
        return df
    # Keep rows where any of the selected values are in the list
    return df[df[col].apply(lambda x: any(item in (x if isinstance(x, list) else [x]) for item in selected))]


@app.callback(
    [Output('map-graph', 'figure'),
     Output('project-table', 'data')],
    [Input('country-dropdown', 'value'),
     Input('per-capita-toggle', 'value'),
     Input('funding-scheme-dropdown', 'value'),
     Input('field-class-dropdown', 'value'),
     Input('field-dropdown', 'value'),
     Input('subfield-dropdown', 'value'),
     Input('niche-dropdown', 'value'),
     Input('activity-type-dropdown', 'value'),
     Input('role-dropdown', 'value'),
     Input('year-dropdown', 'value'),
     Input('network-toggle', 'value'),
     Input('org-pins-toggle', 'value')]
)

def to_list_of_strings(val):
    if isinstance(val, list):
        # Convert all elements to string
        return [str(x).strip() for x in val if pd.notnull(x)]
    if isinstance(val, str):
        # Remove brackets if present and split by comma
        val = val.strip()
        if val.startswith('[') and val.endswith(']'):
            val = val[1:-1]
        # Split by comma and strip whitespace
        return [v.strip() for v in val.split(',') if v.strip()]
    if pd.isnull(val):
        return []
    return [str(val).strip()]

def update_map(selected_country, 
               per_capita_toggle, 
               selected_funding_scheme, 
               selected_field_class,
               selected_field,
               selected_subfield,
               selected_niche,
               selected_activity_type,
               selected_role,
               selected_year,
               network_toggle,
               org_pins_toggle):
    
    # Filter df_proj and df_org by funding_scheme if not 'all'
    if selected_funding_scheme and selected_funding_scheme != 'all':
        filtered_proj = df_proj[df_proj['funding_scheme'] == selected_funding_scheme]
        filtered_org = df_org[df_org['id'].isin(filtered_proj['id'])]
    else:
        filtered_proj = df_proj
        filtered_org = df_org
 

    # filter by thematic fields
    for col in ['field_class', 'field', 'sub_field', 'niche']:
        filtered_proj[col] = filtered_proj[col].apply(to_list_of_strings)
    filtered_proj = filter_by_list_column(filtered_proj, 'field_class', selected_field_class)
    filtered_proj = filter_by_list_column(filtered_proj, 'field', selected_field)
    filtered_proj = filter_by_list_column(filtered_proj, 'sub_field', selected_subfield)
    filtered_proj = filter_by_list_column(filtered_proj, 'niche', selected_niche)
    filtered_org = filtered_org[filtered_org['id'].isin(filtered_proj['id'])]



    # Filter by activity type
    if selected_activity_type and selected_activity_type != 'all':
        filtered_org = filtered_org[filtered_org['activity_type'] == selected_activity_type]

    # Filter by organization role
    if selected_role and selected_role != 'all':
        filtered_org = filtered_org[filtered_org['role'] == selected_role]

    # Filter by starting year
    if selected_year and selected_year != 'all':
        filtered_proj = filtered_proj[filtered_proj['start_year'] == int(selected_year)]
        filtered_org = filtered_org[filtered_org['id'].isin(filtered_proj['id'])]

    if 'show_network' in network_toggle:
        fig = plot_collaboration_network(filtered_org, filtered_proj.head(10))
        
    
    # Recompute country_summary with the filtered data
    country_summary = (
        filtered_org
        .merge(filtered_proj, on='id')
        .groupby('country')
        .agg(
            total_contribution=('ec_max_contribution', 'sum'),
            project_count=('id', 'count')
        )
        .reset_index()
    )
     
    country_summary['iso_alpha_3'] = country_summary['country'].apply(iso2_to_iso3)
    country_summary['latitude'] = country_summary['iso_alpha_3'].map(lambda x: country_centroids_iso3.get(x, (None, None))[0])
    country_summary['longitude'] = country_summary['iso_alpha_3'].map(lambda x: country_centroids_iso3.get(x, (None, None))[1])
    country_summary['log_contribution'] = np.log10(country_summary['total_contribution'] + 1)

    if '€/100k_inhabitants' not in country_summary.columns:
        country_summary['€/100k_inhabitants'] = country_summary['iso_alpha_3'].map(
            lambda iso3: country_summary.loc[country_summary['iso_alpha_3'] == iso3, 'total_contribution'].values[0] / country_populations.get(iso3, 1) * 100_000
            if country_populations.get(iso3, 0) > 0 else 0
        )
    country_summary['log_contribution_per_100k'] = np.log10(country_summary['€/100k_inhabitants'] + 1)

    if selected_country == 'all':
        color_col = 'log_contribution_per_100k' if 'per_capita' in per_capita_toggle else 'log_contribution'
        fig = px.scatter_map(
            country_summary,
            lat='latitude',
            lon='longitude',
            size='total_contribution',
            hover_name='country',
            color=color_col,
            # projection='natural earth',
            title='Total EC Contribution by Country',
            width=1600,    # 4:3 aspect ratio
            height=900, 
            zoom=2
        )
        fig.update_layout(geo=dict(showcountries=True))
        
        # --- Add pins for all organizations left after filtering ---
        if org_pins_toggle and 'show_pins' in org_pins_toggle:
            orgs_with_coords = filtered_org.dropna(subset=['latitude', 'longitude'])
            fig.add_trace(
                go.Scattermap(
                    lon=orgs_with_coords['longitude'],
                    lat=orgs_with_coords['latitude'],
                    text=orgs_with_coords['name'],
                    mode='markers',
                    marker=dict(size=6, color='red', symbol='circle'),
                    name='Organizations'
                )
            )
    else:
        orgs_in_country = filtered_org[filtered_org['iso_alpha_3'] == selected_country].copy()
        orgs_in_country = orgs_in_country.merge(df_proj, on='id')

        city_summary = (
            orgs_in_country
            .groupby(['name', 'latitude', 'longitude'], dropna=True)
            .agg(total_contribution=('ec_max_contribution', 'sum'))
            .reset_index()
        )

        fig = px.density_mapbox(
            city_summary,
            lat='latitude',
            lon='longitude',
            z='total_contribution',
            radius=15,
            center=dict(
                lat=country_centroids_iso3[selected_country][0],
                lon=country_centroids_iso3[selected_country][1]
            ),
            zoom=5,
            mapbox_style="open-street-map",
            title=f"City-level EC Contribution in {selected_country}"
        )
        fig.update_layout(
            uirevision='citymap',
            dragmode='zoom'
        )

        # --- Add pins for all organizations left after filtering ---
        if org_pins_toggle and 'show_pins' in org_pins_toggle:
            orgs_with_coords = filtered_org.dropna(subset=['latitude', 'longitude'])
            fig.add_trace(
                go.Scattermap(
                    lon=orgs_with_coords['longitude'],
                    lat=orgs_with_coords['latitude'],
                    text=orgs_with_coords['name'],
                    mode='markers',
                    marker=dict(size=6, color='red', symbol='circle'),
                    name='Organizations'
                )
            )
    
    # Prepare project table data
    if selected_country == 'all':
        filtered_projects = filtered_proj.copy()
    else:
        orgs_in_country = filtered_org[filtered_org['iso_alpha_3'] == selected_country]
        project_ids = orgs_in_country['id'].unique()
        filtered_projects = filtered_proj[filtered_proj['id'].isin(project_ids)]

    # Get involved institutes as a comma-separated string
    institutes_per_project = (
        filtered_org.groupby('id')['name']
        .apply(lambda names: ', '.join(sorted(names.unique())))
        .to_dict()
    )

    table_data = []
    for _, row in filtered_projects.iterrows():
        table_data.append({
            "project_acronym": row.get("acronym", ""),
            "ec_max_contribution": row.get("ec_max_contribution", 0),
            "title": row.get("title", ""),
            "institutes": institutes_per_project.get(row['id'], "")
        })

    return fig, table_data
        
if __name__ == '__main__':
    app.run(debug=True, port=8040)
