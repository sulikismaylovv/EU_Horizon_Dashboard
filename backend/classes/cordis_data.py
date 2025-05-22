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
        '''
        Initialize class: load data from the CSV files

        set some global variables that we 
        enrich (bool): if True, run the enrchment feautures. If False, load the processed data

        Parameters:
        - parent_dir: should be path to EU_Dashboard repo. From here on paths are specified
        - enrich: if True, run the enrichment features. If False, load the processed data

        '''
        self.parent_dir = parent_dir
        self.raw_dir = f'{parent_dir}/data/raw'
        self.interim_dir = f'{parent_dir}/data/interim'
        self.processed_dir = f'{parent_dir}/data/processed'

        # define some file paths
        SciVoc_path = f'{self.raw_dir}/euroSciVoc.csv'
        legalBasis_path = f'{self.raw_dir}/legalBasis.csv'
        organization_path = f'{self.raw_dir}/organization.csv'
        project_path = f'{self.raw_dir}/project.csv'
        topics_path = f'{self.raw_dir}/topics.csv'
        webItems_path = f'{self.raw_dir}/webItem.csv'
        webLink_path = f'{self.raw_dir}/webLink.csv'

        # Load all datasets and set as class attributes
        #self.data_report = pd.read_csv(f'{self.interim_dir}/project_interim.csv', delimiter=';')
        self.data_deliverables = pd.read_csv(f'{self.interim_dir}/projectDeliverables_interim.csv', delimiter=';')
        self.data_publications = pd.read_csv(f'{self.interim_dir}/projectPublications_interim.csv', delimiter=';')
        self.project_df = pd.read_csv(f'{self.interim_dir}/project_interim.csv', delimiter=';')
        self.sci_voc_df = pd.read_csv(SciVoc_path, delimiter=';')
        self.legal_basis_df = pd.read_csv(legalBasis_path, delimiter=';')   
        self.organization_df = pd.read_csv(organization_path, delimiter=';')
        self.topics_df = pd.read_csv(topics_path, delimiter=';')
        self.web_items_df = pd.read_csv(webItems_path, delimiter=';')
        self.web_link_df = pd.read_csv(webLink_path, delimiter=';')

        self.project_df = robust_csv_reader(project_path, delimiter=';')
        if enrich:
            # enrich the project DataFrame with some additional information
            # Call enrichment functions
            self._enrich_temporal_features()
            self._enrich_people_and_institutions()
            self._enrich_financial_metrics()
            self._enrich_scientific_thematic()

        # Extract possible scientific fields
        self.scientific_fields = self.extract_scientific_fields()
    
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

        # 4) Write back
        self.project_df = df




    def get_projects_by_scientific_field(self):
        """
        Get a list of all projects filtered by the scientific field. 
        """
        # initialize empty dictionary with list in which we store project acronyms
        projects_per_field = {}
        for field in self.scientific_fields:
            projects_per_field[str(field)] = []

        # Go through  sciVoc dataframe and add acronym to the list
        for i in range(len(self.sci_voc_df)):
            for field in self.scientific_fields:
                if field in self.sci_voc_df['euroSciVocPath'][i]:
                    project_id = self.sci_voc_df['projectID'][i]
                    acronym = self.project_df[self.project_df['id'] == project_id]['acronym'].values[0]
                    projects_per_field[field].append(acronym)
                
        # Remove duplicates
        for field in self.scientific_fields:
            projects_per_field[field] = list(set(projects_per_field[field]))
        return projects_per_field

    def get_projects_by_institution(self, institution_keyword):
        filtered = self.organization_df[self.organization_df['name'].str.contains(institution_keyword, case=False, na=False)]
        acronyms = filtered['projectAcronym'].dropna().unique().tolist()
        return acronyms
    
    def extract_scientific_fields(self):
        paths = self.sci_voc_df['euroSciVocPath'].dropna().unique()
        fields = set()
        for path in paths:
            segments = path.strip('/').split('/')
            if segments:
                fields.add(segments[0])
        return sorted(fields)

    def export_dataframes(self, directory, format='csv', include_all=False):
        """
        Export enriched project_df and optionally all loaded dataframes.

        Parameters:
        - directory: str. Path where files will be saved.
        - format: 'csv' or 'excel' (default: 'csv')
        - include_all: if True, export all loaded dataframes; else only project_df
        """
        def _save(df, name):
            path = os.path.join(directory, f"{name}.{ext}")
            if format == 'csv':
                df.to_csv(path, index=False)

        if format == 'csv':
            ext = 'csv'
        else:
            print('Use CSV dumbass. Proceeding to store the data as CSV files.')
            ext = 'csv'
        _save(self.project_df, "project_df")

        if include_all:
            _save(self.data_deliverables, "data_deliverables")
            _save(self.data_publications, "data_publications")
            _save(self.organization_df, "organization_df")
            _save(self.legal_basis_df, "legal_basis_df")
            _save(self.topics_df, "topics_df")
            _save(self.sci_voc_df, "sci_voc_df")
            _save(self.web_items_df, "web_items_df")
            _save(self.web_link_df, "web_link_df")