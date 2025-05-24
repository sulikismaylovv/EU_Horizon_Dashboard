import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import pandas as pd
import pycountry
from itertools import combinations
from collections import Counter


class CORDISPlots:
    """
    Generic plotting class built on top of the processed Horizon datasets.

    Included plots:
      - Total EC contribution by country
      - Number of projects per country
      - Top institutions by funding
      - Distribution of EC funding per project
      - Collaboration network of institutions
      - Funding over time per scientific field
      - EU funding choropleth by country
    """

    def __init__(self, cordis_data):
        """
        :param cordis_data: a CORDIS_data instance with processed CSVs loaded.
        """
        self.data = cordis_data

    def ec_contribution_by_country(self):
        # join the fact‐table → org dim to get country
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
        # count unique project_ids per country via the join
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
        # sum ec_contribution per institution name via the join
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


    # Distribution of EC funding per project
    # ------------------------------------
    def funding_distribution_per_project(self):
        return px.histogram(
            self.data.project_df,
            x="ec_max_contribution",
            nbins=20,
            title="Distribution of EC Funding per Project",
            labels={"ec_max_contribution":"EC Funding (EUR)"}
        )
        
        
    # Collaboration network of institutions
    # ------------------------------------
    def plot_collaboration_network(
        self,
        field_filter: str = None,
        org_types: list = None,
        max_projects: int = 1000,
        min_participants: int = 2,
        countries: list = None,
        year: int = None,
        contribution: float = None,
        project_type: list = None,
        disciplines: list = None
    ) -> go.Figure:
        """
        Plot the institution collaboration network.
        
        :param field_filter: include projects whose any of [field_class, field, sub_field, niche]
                             contains this substring (case‐insensitive)
        :param org_types: list of activity_type codes to include
        :param max_projects: cap number of projects to include
        :param min_participants: minimum distinct institutions per project
        :param countries: list of country codes to include
        :param year: integer year to filter by project start_date
        :param contribution: minimal ec_max_contribution to include
        :param project_type: list of funding_scheme substrings to include (case‐insensitive)
        :param disciplines: list of topic_titles to include (matches any in topic_titles list‐column)
        """
        # 1) prepare projects
        df_proj = (
            self.data.project_df
                .rename(columns={'id':'project_id'})
                .copy()
        )

        # helper: early‐exit “no data” figure
        def _empty_fig(msg):
            return go.Figure(
                layout=go.Layout(
                    annotations=[dict(text=msg, x=0.5, y=0.5,
                                      showarrow=False, font=dict(size=16))],
                    xaxis={'visible':False}, yaxis={'visible':False}
                )
            )

        # 2) project‐level filtering
        if field_filter:
            ff = field_filter.lower()
            mask = pd.Series(False, index=df_proj.index)
            # each of these columns currently holds a string like "['foo','bar']"
            for lvl in ('field_class','field','sub_field','niche'):
                if lvl in df_proj.columns:
                    mask |= (
                        df_proj[lvl]
                           .astype(str)                # ensure we have strings
                           .str.lower()                # lowercase
                           .str.contains(ff, na=False) # substring match
                    )
            df_proj = df_proj[mask]

        if project_type:
            pats = [pt.lower() for pt in project_type]
            df_proj = df_proj[df_proj['funding_scheme']
                                .astype(str)
                                .str.lower()
                                .apply(lambda s: any(p in s for p in pats))]

        if year:
            df_proj['start_date'] = pd.to_datetime(df_proj['start_date'], errors='coerce')
            df_proj = df_proj[df_proj['start_date'].dt.year == int(year)]

        if contribution:
            df_proj = df_proj[df_proj['ec_max_contribution'] >= float(contribution)]

        if disciplines and 'topic_titles' in df_proj:
            disc = {d.lower() for d in disciplines}
            df_proj = df_proj[df_proj['topic_titles']
                                .apply(lambda L: any(v.lower() in disc for v in L if isinstance(v, str)))]

        if df_proj.empty:
            return _empty_fig("⚠️ No projects match those filters")

        keep_pids = set(df_proj['project_id'])

        # 3) join project↔org
        df_rel = self.data.project_organizations
        df_org = self.data.organization_df.rename(columns={'id':'organization_id'})
        df = (
            df_rel
              .merge(df_org, on='organization_id', how='inner')
              .query("project_id in @keep_pids")
        )

        # 4) org‐level filters
        if org_types:
            df = df[df['activity_type'].isin(org_types)]
        if countries:
            df = df[df['country'].isin(countries)]
        if df.empty:
            return _empty_fig("⚠️ No organizations remain after filtering")

        # 5) build list of unique institutions per project
        collab = (
            df.groupby('project_id')['name']
              .agg(lambda names: list(set(names)))
              .reset_index(name='institutions')
        )
        collab['n_inst'] = collab['institutions'].str.len()
        collab = collab[collab['n_inst'] >= min_participants].head(max_projects)
        if collab.empty:
            return _empty_fig("⚠️ No collaborations with ≥ min_participants")

        # 6) count pairwise edges
        edges = Counter()
        for insts in collab['institutions']:
            edges.update(combinations(insts, 2))

        # 7) build NetworkX graph
        G = nx.Graph()
        for (u, v), w in edges.items():
            G.add_edge(u, v, weight=w)

        # 8) layout
        pos = nx.spring_layout(G, k=0.15, iterations=20)

        # 9a) edge trace
        edge_x, edge_y = [], []
        for u, v in G.edges():
            x0, y0 = pos[u]; x1, y1 = pos[v]
            edge_x += [x0, x1, None]
            edge_y += [y0, y1, None]
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=0.5, color='#888'),
            hoverinfo='none'
        )

        # 9b) node trace
        node_x, node_y, node_text = [], [], []
        for n in G.nodes():
            x, y = pos[n]
            node_x.append(x); node_y.append(y)
            node_text.append(n)
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            text=node_text, textposition='top center',
            marker=dict(size=10, line_width=2, color='blue'),
            hoverinfo='text'
        )

        title = f'Collaboration Network{" for "+field_filter if field_filter else ""}'
        return go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                title=title,
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20, l=5, r=5, t=40),
                xaxis=dict(showgrid=False, zeroline=False),
                yaxis=dict(showgrid=False, zeroline=False)
            )
        )

    
    def plot_funding_over_time_by_field(self):
        """
        Line plot of total EC funding per year, broken out by top-level field_class.
        """
        df = self.data.project_df.copy()

        # debug
        print("Available columns in project_df:", df.columns.tolist())

        if "start_date" not in df.columns or "field_class" not in df.columns:
            raise KeyError(
                "Required columns 'start_date' and 'field_class' not found in project_df."
            )

        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df = df.dropna(subset=["start_date"])

        df["year"] = df["start_date"].dt.year

        # explode list-column
        df = df.explode("field_class")

        df_grouped = (
            df.groupby(["year","field_class"], as_index=False)["ec_max_contribution"]
              .sum()
        )

        return px.line(
            df_grouped,
            x="year",
            y="ec_max_contribution",
            color="field_class",
            title="Funding Over Time per Scientific Field",
            labels={
                "ec_max_contribution":"Funding (EUR)",
                "year":"Year",
                "field_class":"Scientific Field"
            }
        )

    def plot_funding_per_country_choropleth(self):
        """
        Choropleth of total EC funding by country (ISO-3), 
        summing contributions from the project_organizations fact table.
        """
        # helper to convert ISO-2 → ISO-3
        def _to_iso3(a2):
            try:
                return pycountry.countries.get(alpha_2=a2).alpha_3
            except:
                return None

        # 1) join fact table to org dimension to get country
        df = (
            self.data.project_organizations
                .merge(
                    self.data.organization_df[['id','country']],
                    left_on='organization_id',
                    right_on='id',
                    how='left'
                )
        )

        # 2) sum up ec_contribution by country
        dfc = (
            df
            .groupby('country', as_index=False)['ec_contribution']
            .sum()
            .rename(columns={'ec_contribution':'funding'})
        )

        # 3) map to ISO-3 codes and drop any unknowns
        dfc['iso3'] = dfc['country'].apply(_to_iso3)
        dfc = dfc.dropna(subset=['iso3'])

        # 4) build the choropleth
        fig = px.choropleth(
            dfc,
            locations='iso3',
            locationmode='ISO-3',
            color='funding',
            color_continuous_scale='Viridis',
            labels={'funding':'EC Contribution (EUR)'},
            title='Total EC Contribution by Country'
        )
        return fig

