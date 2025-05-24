import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import pandas as pd
from plotly.subplots import make_subplots
import pycountry
import numpy as np


# Define plotting class
class CORDISPlots:
    '''
    Generic plotting class build on top of the HORIZON datasets

    Included plots are:
    - total EC contribution by country
    - number of projects per country
    - top institutions by funding
    - distribution of EC funding per project
    - collaboration network of institutions


    '''
    def __init__(self, cordis_data):
        """
        Initialize with a CORDIS_data instance containing all datasets.
        """
        self.data = cordis_data

    def ec_contribution_by_country(self):
        df = (
            self.data.organization_df.groupby("country")["ecContribution"]
            .sum()
            .reset_index()
            .sort_values(by="ecContribution", ascending=False)
        )
        return px.bar(df, x="country", y="ecContribution", title="Total EC Contribution by Country")

    def projects_per_country(self):
        df = (
            self.data.organization_df.groupby("country")["projectID"]
            .nunique()
            .reset_index(name="project_count")
            .sort_values(by="project_count", ascending=False)
        )
        return px.bar(df, x="country", y="project_count", title="Number of Projects per Country")

    def top_institutions_by_funding(self, top_n=15):
        df = (
            self.data.organization_df.groupby("name")["ecContribution"]
            .sum()
            .reset_index()
            .sort_values(by="ecContribution", ascending=False)
            .head(top_n)
        )
        return px.bar(df, x="name", y="ecContribution", title=f"Top {top_n} Institutions by EC Contribution")

    def funding_distribution_per_project(self):
        return px.histogram(
            self.data.project_df,
            x="ecMaxContribution",
            nbins=20,
            title="Distribution of EC Funding per Project"
        )


    def plot_collaboration_network(self, 
                                   field_filter=None, 
                                   org_types=None, 
                                   max_projects=1000, 
                                   min_participants=2, 
                                   countries=None,
                                   year=None,
                                   project_type=None):
        """
        Plot institution collaboration network.
        Now uses project_organizations + organizations + project_df with your actual columns:
          - project_df has: id, field (list), funding_scheme, start_date, etc.
          - project_organizations has: project_id, organization_id
          - organizations has: id, name, activity_type, country
        """
        # 1) load and rename for clarity
        df_proj = self.data.project_df.copy()
        df_proj = df_proj.rename(columns={'id':'project_id'})
        
        df_rel = self.data.project_organizations.copy()
        df_org = self.data.organization_df.copy()
        
        # 2) merge join‐table + metadata
        df_join = (
            df_rel
            .merge(df_org[['id','name','activity_type','country']],
                   left_on='organization_id', right_on='id',
                   suffixes=('','_org'))
            .rename(columns={'name':'institution','activity_type':'org_type'})
        )

        # 3) filter projects by thematic field
        if field_filter:
            df_proj = df_proj[
                df_proj['field'].apply(lambda L: field_filter in L if isinstance(L, list) else False)
            ]

        # 4) filter projects by type (HORIZON calls)
        if project_type:
            df_proj = df_proj[
                df_proj['funding_scheme'].astype(str).isin(project_type)
            ]

        # 5) filter by year (start_date is a datetime)
        if year:
            df_proj = df_proj[
                pd.to_datetime(df_proj['start_date'], errors='coerce').dt.year == int(year)
            ]
        
        # 6) now restrict the join‐table to only those projects
        df_join = df_join[df_join['project_id'].isin(df_proj['project_id'])]

        # 7) filter by organization types
        if org_types:
            df_join = df_join[df_join['org_type'].astype(str).isin(org_types)]

        # 8) filter by country
        if countries:
            df_join = df_join[df_join['country'].astype(str).isin(countries)]

        #  9) build list of participants per project
        collab_df = (
            df_join
            .groupby('project_id')
            .agg({'institution': lambda names: list(set(names))})
            .reset_index()
        )
        # make sure our list‐column is called “institutions”
        collab_df = collab_df.rename(columns={'institution':'institutions'})

        # 10) drop small collaborations
        collab_df['n_inst'] = collab_df['institutions'].apply(len)
        collab_df = collab_df[collab_df['n_inst'] >= min_participants]

        # 11) limit to max_projects
        collab_df = collab_df.head(max_projects)

        # now collab_df definitely has an “institutions” column
        from itertools import combinations
        from collections import Counter
        edges = Counter()
        for insts in collab_df['institutions']:
            edges.update(combinations(insts, 2))


        # 12) build graph
        G = nx.Graph()
        for (u,v), w in edges.items():
            G.add_edge(u, v, weight=w)

        pos = nx.spring_layout(G, k=0.15, iterations=20)

        # 13) edge traces
        edge_x, edge_y = [], []
        for u,v in G.edges():
            x0,y0 = pos[u]; x1,y1 = pos[v]
            edge_x += [x0, x1, None]
            edge_y += [y0, y1, None]
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=0.5,color='#888'),
            hoverinfo='none'
        )

        # 14) node traces
        node_x, node_y, node_text = [], [], []
        for node in G.nodes():
            x,y = pos[node]
            node_x.append(x); node_y.append(y)
            node_text.append(node)
        node_trace = go.Scatter(
            x=node_x, y=node_y, mode='markers+text',
            text=node_text, textposition='top center',
            marker=dict(size=10, line_width=2, color='blue')
        )

        title = (f'Collaboration Network for "{field_filter}"'
                 if field_filter else 'Institution Collaboration Network')
        fig = go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                title=title,
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                xaxis=dict(showgrid=False,zeroline=False),
                yaxis=dict(showgrid=False,zeroline=False)
            )
        )
        return fig

    def plot_funding_over_time_by_field(self):
        '''
        Function that returns figure with the total yearly amount of money allocated to high-level scientific fields
        '''
        df = self.data.project_df.copy()

        if 'startDate' not in df.columns or 'sci_voc_paths' not in df.columns:
            raise KeyError("Required columns 'startDate' and 'sci_voc_paths' not found in project_df.")

        df = df.dropna(subset=['startDate', 'sci_voc_paths'])
        
        # Get starting year of project
        df['startDate'] = pd.to_datetime(df['startDate'], errors='coerce')
        df['year'] = df['startDate'].dt.year

        # Split paths to extract top-level category (e.g., "/natural sciences/...")
        df['top_field'] = df['sci_voc_paths'].apply(lambda x: x[0].split('/')[1] if isinstance(x[0], str) and '/' in x[0] else 'Unknown')
        df_grouped = df.groupby(['year', 'top_field'])['ecMaxContribution'].sum().reset_index()

        
        fig = px.line(df_grouped, x='year', y='ecMaxContribution', color='top_field',
                    title='Funding Over Time per Scientific Field',
                    labels={'ecMaxContribution': 'Funding (EUR)', 'year': 'Year', 'top_field': 'Scientific Field'})
        return fig
    
    def plot_funding_per_country_choropleth(self):
        '''
        Plot the total EU funding by country using a chlorlopleth map.
        For this we use the plotly express library
        '''
        def convert_iso2_to_iso3(code):
            try:
                return pycountry.countries.get(alpha_2=code).alpha_3
            except:
                return None
        df = self.data.organization_df
        df_grouped = df.groupby('country', as_index=False)['ecContribution'].sum()
        df_grouped['iso_alpha_3'] = df_grouped['country'].apply(convert_iso2_to_iso3)
        df_grouped = df_grouped.dropna(subset=['iso_alpha_3'])  # Drop rows where ISO-3 conversion failed

        fig = px.choropleth(df_grouped,
                                locations='iso_alpha_3',
                                locationmode='ISO-3',
                                color='ecContribution',
                                color_continuous_scale='Viridis',
                                labels={'ecContribution': 'Funding (EUR)'},
                                title='Total EU Funding by Country')
        return fig