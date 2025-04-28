import pandas as pd

def transform_projects(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Join the cleaned project table with topics, legal basis, and org meta:
     - embeds topic-list as a Python list in column `topics`
     - counts number of unique partners as `num_partners`
    """
    project = dfs['project']
    
    # 1) merge legal basis if present
    if 'legalBasis' in dfs:
        legal = dfs['legalBasis'].rename(columns={'projectid':'id'})
        project = project.merge(legal, how='left', on='id', suffixes=('','_legal'))
    
    # 2) aggregate topics
    if 'topics' in dfs:
        topics = dfs['topics']
        topic_list = (
            topics.groupby('project_id')['topic']
                  .agg(list)
                  .rename('topics')
        )
        project = project.merge(topic_list, how='left', left_on='id', right_index=True)
    
    # 3) partner counts
    if 'organization' in dfs:
        org = dfs['organization']
        partner_cnt = (
            org.groupby('project_id')['organisation_id']
               .nunique()
               .rename('num_partners')
        )
        project = project.merge(partner_cnt, how='left', left_on='id', right_index=True)
    
    return project

def transform_deliverables(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # take the largest table as “main”
    return max(dfs.values(), key=lambda df: df.shape[0])

def transform_summaries(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return max(dfs.values(), key=lambda df: df.shape[0])

def transform_publications(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return max(dfs.values(), key=lambda df: df.shape[0])
