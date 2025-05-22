import pandas as pd

def transform_projects(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    project = dfs['project']
    # Join legal basis
    if 'legalbasis' in dfs:
        legal = dfs['legalbasis']
        project = project.merge(legal, how='left', left_on='id', right_on='project_id', suffixes=('','_legal'))
    # Aggregate topics
    if 'topics' in dfs:
        topics = dfs['topics']
        topic_list = (
            topics.groupby('project_id')['topic']
            .agg(lambda x: list(x.dropna().unique()) if x.notnull().any() else [])
            .rename('topics')
        )
        project = project.merge(topic_list, how='left', left_on='id', right_index=True)
        project['topics'] = project['topics'].apply(lambda x: x if isinstance(x, list) else [])
    # Partner counts
    if 'organization' in dfs:
        org = dfs['organization']
        partner_cnt = (
            org.groupby('project_id')['organization_id']
            .nunique()
            .rename('num_partners')
        )
        project = project.merge(partner_cnt, how='left', left_on='id', right_index=True)
        project['num_partners'] = project['num_partners'].fillna(0).astype(int)
    # Drop duplicates
    project = project.drop_duplicates(subset='id')
    return project.reset_index(drop=True)


def transform_deliverables(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # take the largest table as “main”
    return max(dfs.values(), key=lambda df: df.shape[0])

def transform_summaries(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return max(dfs.values(), key=lambda df: df.shape[0])

def transform_publications(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return max(dfs.values(), key=lambda df: df.shape[0])


