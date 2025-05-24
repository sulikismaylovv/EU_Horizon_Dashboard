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
        """
        Plot the institution collaboration network.

        :param field_filter: only include projects whose `field` list contains this value
        :param org_types: list of activity_type codes to include (e.g. ['HES','REC','SME'])
        :param max_projects: cap number of projects to include (head)
        :param min_participants: minimum distinct institutions per project
        :param countries: list of country codes to include
        :param year: integer year to filter by project start_date
        :param project_type: list of funding_scheme strings to include
        :returns: Plotly Figure
        """
        # 1) load and rename project key
        df_proj = self.data.project_df.rename(columns={"id":"project_id"})

        # 2) load the join-table and org metadata
        df_rel = self.data.project_organizations
        df_org = self.data.organization_df

        df = (
            df_rel
            .merge(
                df_org[["id","name","activity_type","country"]],
                left_on="organization_id", right_on="id",
                suffixes=("","_org")
            )
            .rename(columns={"name":"institution", "activity_type":"org_type"})
        )

        # 3) apply project-level filters
        if field_filter:
            df_proj = df_proj[df_proj["field"].apply(lambda L: field_filter in L if isinstance(L,list) else False)]
        if project_type:
            df_proj = df_proj[df_proj["funding_scheme"].astype(str).isin(project_type)]
        if year:
            df_proj = df_proj[pd.to_datetime(df_proj["start_date"],errors="coerce").dt.year == int(year)]

        # restrict join-table to those project_ids
        df = df[df["project_id"].isin(df_proj["project_id"])]

        # 4) apply org-level filters
        if org_types:
            df = df[df["org_type"].isin(org_types)]
        if countries:
            df = df[df["country"].isin(countries)]

        # 5) build list of unique institutions per project
        collab = (
            df.groupby("project_id")["institution"]
              .apply(lambda names: list(set(names)))
              .reset_index(name="institutions")
        )
        collab["n_inst"] = collab["institutions"].str.len()
        collab = collab[collab["n_inst"] >= min_participants].head(max_projects)

        # 6) build edge list
        edges = Counter()
        for insts in collab["institutions"]:
            edges.update(combinations(insts, 2))

        # 7) build networkx graph
        G = nx.Graph()
        for (u,v), w in edges.items():
            G.add_edge(u, v, weight=w)

        pos = nx.spring_layout(G, k=0.15, iterations=20)

        # 8) edge trace
        edge_x, edge_y = [], []
        for u,v in G.edges():
            x0,y0 = pos[u]; x1,y1 = pos[v]
            edge_x += [x0, x1, None]
            edge_y += [y0, y1, None]
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            mode="lines",
            line=dict(width=0.5, color="#888"),
            hoverinfo="none"
        )

        # 9) node trace
        node_x, node_y, node_text = [], [], []
        for n in G.nodes():
            x,y = pos[n]
            node_x.append(x); node_y.append(y)
            node_text.append(n)
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode="markers+text",
            text=node_text, textposition="top center",
            marker=dict(size=10, line_width=2, color="blue")
        )

        title = (
            f'Collaboration Network for "{field_filter}"' 
            if field_filter else
            "Institution Collaboration Network"
        )
        return go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                title=title,
                showlegend=False,
                hovermode="closest",
                margin=dict(b=20,l=5,r=5,t=40),
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

