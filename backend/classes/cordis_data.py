"""
Python file containing the CORDIS_data class. 
it  used to load an process the data from the CSV files in the CORDIS framework.

CORDIS_data works on the complete CORDIS dataset and loads all datasets, enriches them by
combining information from different datasets and provides some methods to access the projects. 

"""

import os
import pandas as pd
from datetime import datetime

## load personal functions
from backend.etl.ingestion import inspect_bad_lines, auto_fix_row, robust_csv_reader


class CORDIS_data():
    def __init__(self, parent_dir, enrich=True):
        """
        Load either raw+run enrichments (enrich=True) or straight processed CSVs (enrich=False).
        """
        self.parent_dir    = parent_dir
        self.raw_dir       = os.path.join(parent_dir, "data", "raw")
        self.interim_dir   = os.path.join(parent_dir, "data", "interim")
        self.processed_dir = os.path.join(parent_dir, "data", "processed")

        # paths for raw
        project_path      = os.path.join(self.raw_dir, "project.csv")
        SciVoc_path       = os.path.join(self.raw_dir, "euroSciVoc.csv")
        legalBasis_path   = os.path.join(self.raw_dir, "legalBasis.csv")
        organization_path = os.path.join(self.raw_dir, "organization.csv")
        topics_path       = os.path.join(self.raw_dir, "topics.csv")
        webItems_path     = os.path.join(self.raw_dir, "webItem.csv")
        webLink_path      = os.path.join(self.raw_dir, "webLink.csv")

        if enrich:
            # — load and enrich from raw/interim
            self.data_deliverables   = pd.read_csv(os.path.join(self.interim_dir, "projectDeliverables_interim.csv"), delimiter=";")
            self.data_publications   = pd.read_csv(os.path.join(self.interim_dir, "projectPublications_interim.csv"), delimiter=";")
            # robust load of project
            self.project_df          = robust_csv_reader(project_path, delimiter=";")
            self.organization_df     = pd.read_csv(organization_path, delimiter=";")
            self.topics_df           = pd.read_csv(topics_path, delimiter=";")
            self.legal_basis_df      = pd.read_csv(legalBasis_path, delimiter=";")
            self.sci_voc_df          = pd.read_csv(SciVoc_path, delimiter=";")
            self.web_items_df        = pd.read_csv(webItems_path, delimiter=";")
            self.web_link_df         = pd.read_csv(webLink_path, delimiter=";")

            # run your enrichment pipeline
            self._enrich_temporal_features()
            self._enrich_people_and_institutions()
            self._enrich_financial_metrics()
            self._enrich_scientific_thematic()
        else:
            # — straight load from processed_dir
            P = self.processed_dir
            # core
            self.project_df              = pd.read_csv(os.path.join(P, "projects.csv"))
            self.data_deliverables       = pd.read_csv(os.path.join(P, "deliverables.csv"))
            self.data_publications       = pd.read_csv(os.path.join(P, "publications.csv"))
            # dimensions
            self.organization_df         = pd.read_csv(os.path.join(P, "organizations.csv"))
            self.topics_df               = pd.read_csv(os.path.join(P, "topics.csv"))
            self.legal_basis_df          = pd.read_csv(os.path.join(P, "legal_basis.csv"))
            self.sci_voc_df              = pd.read_csv(os.path.join(P, "sci_voc.csv"))
            # join tables
            self.project_organizations   = pd.read_csv(os.path.join(P, "project_organizations.csv"))
            self.project_topics          = pd.read_csv(os.path.join(P, "project_topics.csv"))
            self.project_legal_basis     = pd.read_csv(os.path.join(P, "project_legal_basis.csv"))
            self.project_sci_voc         = pd.read_csv(os.path.join(P, "project_sci_voc.csv"))
            self.web_items_df            = pd.read_csv(os.path.join(P, "web_items.csv"))
            self.web_link_df             = pd.read_csv(os.path.join(P, "web_links.csv"))

        # finally compute the available scientific field list
        self.scientific_fields = self.extract_scientific_fields()
        
    # —————————————————————————————————————————————————————————————————————————————
    # public methods
    
    def list_of_acronyms(self, show=True):
        '''
        This function prints out a dataframe 
        '''
        acronyms = pd.DataFrame(self.project_df['acronym'].unique())
        self.acronyms = acronyms
        if show == True:
            return acronyms
    
    def _enrich_temporal_features(self):
        '''
        This function adds temporal features to the project dataframe regarding dates and durations
        '''
        print('Enriching the projects dataset with temporal information.')
        df = self.project_df.copy()
        df['startDate'] = pd.to_datetime(df['startDate'], errors='coerce')
        df['endDate'] = pd.to_datetime(df['endDate'], errors='coerce')
        df['duration_days'] = (df['endDate'] - df['startDate']).dt.days
        
        df['duration_months'] = df['duration_days'] / 30.44
        df['duration_months'] = df['duration_months'].astype(int)

        df['duration_years'] = df['duration_days'] / 365.25
        df['duration_years'] = df['duration_years'].astype(int)
        self.project_df = df
    
    def _enrich_people_and_institutions(self):
        """
        This function adds some information about the people and institutions involved in the project.
        Added features:
        - Number of institutions involved
        - List of institutions involved
        - Coordinator name (if role info is reliable)
        """
        print('Enriching the projects dataset with people and institutions information.')

        # 0) FORCE both merge‐keys to the same dtype:
        #    here we cast everything to string
        self.project_df['id'] = self.project_df['id'].astype(str)
        self.organization_df['projectID'] = self.organization_df['projectID'].astype(str)

        # 1) Count unique institutions per project
        orgs = (
            self.organization_df
            .groupby('projectID')['name']
            .nunique()
            .reset_index(name='n_institutions')
        )

        # 2) List all institutions
        inst_list = (
            self.organization_df
            .groupby('projectID')['name']
            .apply(list)
            .reset_index(name='institutions')
        )

        # 3) Coordinator (if role info is reliable)
        coordinators = self.organization_df[
            self.organization_df['role'].str.lower() == 'coordinator'
        ]
        pi_names = (
            coordinators
            .groupby('projectID')['name']
            .first()
            .reset_index(name='coordinator_name')
        )

        # 4) Merge back onto project_df
        #    Now both 'id' and 'projectID' are strings, so no more ValueError
        self.project_df = (
            self.project_df
            .merge(orgs,      how='left', left_on='id', right_on='projectID')
            .merge(inst_list, how='left', left_on='id', right_on='projectID')
            .merge(pi_names,  how='left', left_on='id', right_on='projectID')
        )


    def _enrich_financial_metrics(self):
        '''
        This function adds some financial metrics to the final project dataframe
        Added feautures:
        - EC contribution per year
        - Total cost per year

        '''
        print('Enriching the projects dataset with financial information.')
        df = self.project_df.copy()

        # Convert to numeric
        df['ecMaxContribution'] = pd.to_numeric(df['ecMaxContribution'], errors='coerce')
        df['totalCost'] = pd.to_numeric(df['totalCost'], errors='coerce')

        # Annualized budget (if duration is available)
        if 'duration_years' not in df.columns:
            self._enrich_temporal_features()
            df = self.project_df

        df['ecContribution_per_year'] = df['ecMaxContribution'] / df['duration_years']
        df['totalCost_per_year'] = df['totalCost'] / df['duration_years']

        self.project_df = df
    


    
    def _enrich_scientific_thematic(self):
        """
        Adds scientific and thematic information to the project dataframe:
        - List of full euroSciVocPath strings per project
        - List of euroSciVocTitle values per project
        - List of topic titles from the topics.csv file
        """
        print('Enriching the projects dataset with thematic / scientific information.')

        # 0) Ensure all keys are strings
        self.project_df['id']            = self.project_df['id'].astype(str)
        self.sci_voc_df['projectID']     = self.sci_voc_df['projectID'].astype(str)
        self.topics_df['projectID']      = self.topics_df['projectID'].astype(str)

        # 1) Build the grouped lookup tables
        sci_paths = (
            self.sci_voc_df
            .groupby('projectID')['euroSciVocPath']
            .apply(list)
            .reset_index(name='sci_voc_paths')
        )
        sci_titles = (
            self.sci_voc_df
            .groupby('projectID')['euroSciVocTitle']
            .apply(list)
            .reset_index(name='sci_voc_titles')
        )
        topic_titles = (
            self.topics_df
            .groupby('projectID')['title']
            .apply(list)
            .reset_index(name='topic_titles')
        )

        # 2) Rename their join-key to 'id' so we can merge cleanly
        sci_paths   = sci_paths.rename(columns={'projectID':'id'})
        sci_titles  = sci_titles.rename(columns={'projectID':'id'})
        topic_titles = topic_titles.rename(columns={'projectID':'id'})

        # 3) Merge them on the common 'id' column — no extra projectID columns, no suffix collisions
        df = self.project_df
        df = df.merge(sci_titles,   how='left', on='id')
        df = df.merge(sci_paths,    how='left', on='id')
        df = df.merge(topic_titles, how='left', on='id')

        # cover missing values
        # sciVoc columns do not cover all projects. We set the NaNs to specific values
        df['sci_voc_titles'] = df['sci_voc_titles'].apply(lambda x: x if isinstance(x, list) else ['other'])
        df['sci_voc_paths'] = df['sci_voc_paths'].apply(lambda x: x if isinstance(x, list) else ['other'])
        
        # add field_class, field, subfield to the DataFrame
        def get_level(x, level):
            '''
            this function checks if the string separated by / has sufficient levels of depth 
            If not, return None
            '''
            parts = x.split('/')
            return parts[level] if len(parts) > level else None

        field_classes = (
            self.sci_voc_df
            .groupby('projectID')['euroSciVocPath']
            .apply(lambda x: list(set(get_level(i, 1) for i in x)))
            .reset_index(name='field_class')
        )

        fields = (
            self.sci_voc_df
            .groupby('projectID')['euroSciVocPath']
            .apply(lambda x: list(set(get_level(i, 2) for i in x if get_level(i, 2) is not None)))
            .reset_index(name='field')
        )

        subfields = (
            self.sci_voc_df
            .groupby('projectID')['euroSciVocPath']
            .apply(lambda x: list(set(get_level(i, 3) for i in x if get_level(i, 3) is not None)))
            .reset_index(name='subfield')
        )

        niche = (
            self.sci_voc_df
            .groupby('projectID')['euroSciVocPath']
            .apply(lambda x: list(set(get_level(i, 4) for i in x if get_level(i, 4) is not None)))
            .reset_index(name='niche')
        )

        # rename identification key
        field_classes   = field_classes.rename(columns={'projectID':'id'})
        fields  = fields.rename(columns={'projectID':'id'})
        subfields = subfields.rename(columns={'projectID':'id'})
        niche = niche.rename(columns={'projectID':'id'})

        # Merge on common identification column
        df = self.project_df
        df = df.merge(field_classes,   how='left', on='id')
        df = df.merge(fields,    how='left', on='id')
        df = df.merge(subfields, how='left', on='id')
        df = df.merge(niche, how='left', on='id')

        # cover missing values
        # Not all projects are present in the SciVoc dataset
        df['field_class'] = df['field_class'].apply(lambda x: x if isinstance(x, list) else ['other'])
        df['field'] = df['field'].apply(lambda x: x if isinstance(x, list) else ['other'])
        df['subfield'] = df['subfield'].apply(lambda x: x if isinstance(x, list) else ['other'])
        df['niche'] = df['niche'].apply(lambda x: x if isinstance(x, list) else ['other'])

        # 4) Write back
        self.project_df = df
        print(f"Enriched project_df with scientific and thematic information: {len(self.project_df)} projects")
        # Print columns of the project dataframe and line separator b efore returning
        print("Columns of the project dataframe after enrichment:")
        print(f"  - {len(df.columns)} columns")
        print("  - Columns: ", end="")
        print(", ".join(df.columns))
        print("==========================================================")

    # —————————————————————————————————————————————————————————————————————————————
    # your enrichment methods remain unchanged …
    # _enrich_temporal_features, _enrich_people_and_institutions,
    # _enrich_financial_metrics, _enrich_scientific_thematic
    # —————————————————————————————————————————————————————————————————————————————


    def get_projects_by_scientific_field(self):
        """
        Returns a dict: { top_level_field : [ project_acronym, … ], … }
        based on the processed project_sci_voc + sci_voc tables.
        """
        # merge project_sci_voc → sci_voc to get each project's path
        df = (
            self.project_sci_voc
              .merge(self.sci_voc_df[["code","path"]],
                     left_on="sci_voc_code", right_on="code",
                     how="left")
        )
        # extract top‐level category before the first "/"
        df["top_field"] = df["path"].fillna("").apply(lambda p: p.strip("/").split("/")[0] if p else "other")

        # map back to acronyms
        result = {}
        for fld, group in df.groupby("top_field"):
            pids = group["project_id"].astype(str).unique()
            acronyms = (
                self.project_df[self.project_df["id"].astype(str).isin(pids)]["acronym"]
                .dropna().unique().tolist()
            )
            result[fld] = acronyms
        return result


    def get_projects_by_institution(self, institution_keyword):
        """
        Returns list of project acronyms for institutions whose name matches keyword.
        """
        # find matching org IDs
        mask = self.organization_df["name"].str.contains(institution_keyword, case=False, na=False)
        org_ids = self.organization_df.loc[mask, "id"].unique()
        # find join rows
        pid = (
            self.project_organizations
              .loc[self.project_organizations["organization_id"].isin(org_ids), "project_id"]
              .astype(str)
        )
        acronyms = (
            self.project_df[self.project_df["id"].astype(str).isin(pid)]
              ["acronym"]
              .dropna()
              .unique()
              .tolist()
        )
        return acronyms


    def extract_scientific_fields(self):
        """
        Returns all distinct top‐level fields from sci_voc_df.path
        """
        col = "path" if "path" in self.sci_voc_df.columns else "euroSciVocPath"
        vals = self.sci_voc_df[col].dropna().unique()
        top_levels = set(v.strip("/").split("/")[0] for v in vals if isinstance(v, str) and "/" in v)
        return sorted(top_levels)
    
    
    def export_raw(self, directory, include_all=False):
        """
        Original export of raw/interim tables (kept for backward compatibility).
        """
        os.makedirs(directory, exist_ok=True)
        self.project_df.to_csv(os.path.join(directory, 'project_df.csv'), index=False)
        if include_all:
            self.data_deliverables.to_csv(os.path.join(directory, 'data_deliverables.csv'), index=False)
            self.data_publications.to_csv(os.path.join(directory, 'data_publications.csv'), index=False)
            self.organization_df.to_csv(os.path.join(directory, 'organization_df.csv'), index=False)
            self.legal_basis_df.to_csv(os.path.join(directory, 'legal_basis_df.csv'), index=False)
            self.topics_df.to_csv(os.path.join(directory, 'topics_df.csv'), index=False)
            self.sci_voc_df.to_csv(os.path.join(directory, 'sci_voc_df.csv'), index=False)
            self.web_items_df.to_csv(os.path.join(directory, 'web_items_df.csv'), index=False)
            self.web_link_df.to_csv(os.path.join(directory, 'web_link_df.csv'), index=False)

import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import pandas as pd
import pycountry
from itertools import combinations
from collections import Counter


class CORDISPlots:
    """
    Plotly-based visualizations over your processed Horizon CSVs.

    Assumes you've loaded, in your CORDIS_data __init__:
      • self.project_df           ← projects.csv
      • self.organization_df      ← organizations.csv
      • self.project_organizations← project_organizations.csv
      • (and the other dims if needed)
    """

    def __init__(self, cordis_data):
        self.data = cordis_data

    def ec_contribution_by_country(self):
        # join project_organizations → organizations to get country + ec_contribution
        df = (
            self.data.project_organizations
              .merge(
                  self.data.organization_df[['id','country']],
                  left_on='organization_id',
                  right_on='id',
                  how='left'
              )
              .groupby('country', as_index=False)['ec_contribution']
              .sum()
              .sort_values('ec_contribution', ascending=False)
        )
        return px.bar(
            df,
            x='country',
            y='ec_contribution',
            title='Total EC Contribution by Country',
            labels={'ec_contribution':'EC Contribution (EUR)'}
        )

    def projects_per_country(self):
        # count distinct projects per country via project_organizations → organizations
        df = (
            self.data.project_organizations
              .merge(
                  self.data.organization_df[['id','country']],
                  left_on='organization_id',
                  right_on='id',
                  how='left'
              )
              .groupby('country', as_index=False)['project_id']
              .nunique()
              .rename(columns={'project_id':'project_count'})
              .sort_values('project_count', ascending=False)
        )
        return px.bar(
            df,
            x='country',
            y='project_count',
            title='Number of Projects per Country'
        )

    def top_institutions_by_funding(self, top_n=15):
        # sum ec_contribution per institution name
        df = (
            self.data.project_organizations
              .merge(
                  self.data.organization_df[['id','name']],
                  left_on='organization_id',
                  right_on='id',
                  how='left'
              )
              .groupby('name', as_index=False)['ec_contribution']
              .sum()
              .sort_values('ec_contribution', ascending=False)
              .head(top_n)
        )
        return px.bar(
            df,
            x='name',
            y='ec_contribution',
            title=f'Top {top_n} Institutions by EC Contribution',
            labels={'ec_contribution':'EC Contribution (EUR)', 'name':'Institution'}
        )

    def funding_distribution_per_project(self):
        return px.histogram(
            self.data.project_df,
            x='ec_max_contribution',
            nbins=20,
            title='Distribution of EC Funding per Project',
            labels={'ec_max_contribution':'EC Funding (EUR)'}
        )

    def plot_collaboration_network(
        self,
        field_filter=None,
        org_types=None,
        max_projects=1000,
        min_participants=2,
        countries=None,
        year=None,
        project_type=None
    ):
        # 1) Prepare projects
        proj = self.data.project_df.rename(columns={'id':'project_id'})
        if field_filter:
            proj = proj[proj['field'].apply(lambda lst: field_filter in lst if isinstance(lst,list) else False)]
        if project_type:
            proj = proj[proj['funding_scheme'].isin(project_type)]
        if year:
            proj = proj[pd.to_datetime(proj['start_date']).dt.year == int(year)]

        # 2) Build join + metadata
        rel = (
            self.data.project_organizations
              .merge(self.data.organization_df[['id','name','activity_type','country']],
                     left_on='organization_id', right_on='id', how='left')
              .rename(columns={'name':'institution','activity_type':'org_type'})
        )
        # restrict to chosen projects
        rel = rel[ rel['project_id'].isin(proj['project_id']) ]
        if org_types:
            rel = rel[ rel['org_type'].isin(org_types) ]
        if countries:
            rel = rel[ rel['country'].isin(countries) ]

        # 3) Aggregate per project
        collab = (
            rel.groupby('project_id')['institution']
               .apply(lambda names: list(set(names)))
               .reset_index(name='institutions')
        )
        collab['n_inst'] = collab['institutions'].str.len()
        collab = collab[collab['n_inst']>=min_participants].head(max_projects)

        # 4) Build edges & Graph
        edges = Counter()
        for insts in collab['institutions']:
            edges.update(combinations(insts,2))

        G = nx.Graph()
        for (u,v),w in edges.items():
            G.add_edge(u,v,weight=w)
        pos = nx.spring_layout(G, k=0.15, iterations=20)

        # 5) Plotly traces
        edge_x, edge_y = [], []
        for u,v in G.edges():
            x0,y0 = pos[u]; x1,y1 = pos[v]
            edge_x += [x0,x1,None]; edge_y += [y0,y1,None]
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y, mode='lines',
            line=dict(width=0.5,color='#888'),
            hoverinfo='none'
        )

        node_x, node_y, node_text = [], [], []
        for n in G.nodes():
            x,y = pos[n]
            node_x.append(x); node_y.append(y); node_text.append(n)
        node_trace = go.Scatter(
            x=node_x, y=node_y, mode='markers+text',
            text=node_text, textposition='top center',
            marker=dict(size=10,line_width=2,color='blue')
        )

        title = (f'Collaboration Network for "{field_filter}"' if field_filter
                 else 'Institution Collaboration Network')
        return go.Figure(
            data=[edge_trace,node_trace],
            layout=go.Layout(
                title=title, showlegend=False, hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                xaxis=dict(showgrid=False,zeroline=False),
                yaxis=dict(showgrid=False,zeroline=False)
            )
        )

    def plot_funding_over_time_by_field(self):
        df = self.data.project_df.copy()
        print("Available columns in project_df:", df.columns.tolist())
        if 'start_date' not in df.columns or 'field_class' not in df.columns:
            raise KeyError("Need 'start_date' & 'field_class' in project_df")

        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df = df.dropna(subset=['start_date'])
        df['year'] = df['start_date'].dt.year

        # each project can belong to multiple field_classes
        df = df.explode('field_class')

        df_grouped = (
            df.groupby(['year','field_class'], as_index=False)['ec_max_contribution']
              .sum()
        )

        return px.line(
            df_grouped,
            x='year', y='ec_max_contribution', color='field_class',
            title='Funding Over Time per Scientific Field',
            labels={
                'ec_max_contribution':'Funding (EUR)',
                'year':'Year',
                'field_class':'Scientific Field'
            }
        )

    def plot_funding_per_country_choropleth(self):
        def to_iso3(a2):
            try:
                return pycountry.countries.get(alpha_2=a2).alpha_3
            except:
                return None

        df = (
            self.data.project_organizations
              .merge(self.data.organization_df[['id','country']],
                     left_on='organization_id', right_on='id', how='left')
              .groupby('country', as_index=False)['ec_contribution']
              .sum()
              .rename(columns={'ec_contribution':'funding'})
        )
        df['iso3'] = df['country'].apply(to_iso3)
        df = df.dropna(subset=['iso3'])

        return px.choropleth(
            df,
            locations='iso3', color='funding',
            locationmode='ISO-3',
            color_continuous_scale='Viridis',
            labels={'funding':'Funding (EUR)'},
            title='Total EU Funding by Country'
        )
