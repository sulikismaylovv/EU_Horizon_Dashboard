"""
Python file containing the CORDIS_data class and Project_data class. 
These are used to load an process the data from the CSV files in the CORDIS framework.

CORDIS_data works on the complete CORDIS dataset and loads all datasets, enriches them by
combining information from different datasets and provides some methods to access the projects. 

Project_data is a subclass of CORDIS_data and is used to extract all inormation related to a single project,
based on the project identifier or acronym. Also, this extracts some additional information from the dataset by 
processing infromation given in the supplemantry CORDIS datasets such as euroSciVoc, legal basis, etc. 
"""


# class to load datasets
# Imports 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
from pprint import pprint
from pathlib import Path
import csv

# define some file paths
run_dir = os.getcwd()
parent_dir = os.path.dirname(run_dir)

raw_dir = f'{parent_dir}/data/raw'
interim_dir = f'{parent_dir}/data/interim'
processed_dir = f'{parent_dir}/data/processed'

# define file paths to project-specific files
data_report_path = f'{raw_dir}/reportSummaries.csv'
data_filereport_path = f'{raw_dir}/file_report.csv'
data_publications_path = f'{raw_dir}/projectPublications.csv'
data_deliverables_path = f'{raw_dir}/projectDeliverables.csv'

# define file paths
CORDIS_framework_docs_dir = f'{raw_dir}/cordis-HORIZONprojects-csv'

SciVoc_path = f'{CORDIS_framework_docs_dir}/euroSciVoc.csv'
legalBasis_path = f'{CORDIS_framework_docs_dir}/legalBasis.csv'
organization_path = f'{CORDIS_framework_docs_dir}/organization.csv'
project_path = f'{CORDIS_framework_docs_dir}/project.csv'
topics_path = f'{CORDIS_framework_docs_dir}/topics.csv'
webItems_path = f'{CORDIS_framework_docs_dir}/webItem.csv'
webLink_path = f'{CORDIS_framework_docs_dir}/webLink.csv'


class CORDIS_data():
    def __init__(self, enrich=True):
        '''
        Initialize class: load data from the CSV files

        set some global variables that we 
        enrich (bool): if True, run the enrchment feautures. If False, load the processed data


        '''
        # Load all datasets and set as class attributes
        self.data_report = pd.read_csv(f'{interim_dir}/projectdeliverables_interim.csv', delimiter=';')
        self.data_deliverables = pd.read_csv(f'{interim_dir}/projectdeliverables_interim.csv', delimiter=';')
        self.data_publications = pd.read_csv(f'{interim_dir}/projectPublications_interim.csv', delimiter=';')
        self.project_df = pd.read_csv(f'{interim_dir}/project_interim.csv', delimiter=';')
        self.sci_voc_df = pd.read_csv(SciVoc_path, delimiter=';')
        self.legal_basis_df = pd.read_csv(legalBasis_path, delimiter=';')   
        self.organization_df = pd.read_csv(organization_path, delimiter=';')
        self.topics_df = pd.read_csv(topics_path, delimiter=';')
        self.web_items_df = pd.read_csv(webItems_path, delimiter=';')
        self.web_link_df = pd.read_csv(webLink_path, delimiter=';')

        if enrich:
            # enrich the project DataFrame with some additional information
            # Call enrichment functions
            self._enrich_temporal_features()
            self._enrich_people_and_institutions()
            self._enrich_financial_metrics()
            self._enrich_scientific_thematic()
        else:
            self.project_df = pd.read_csv(f'{processed_dir}/project_df.csv', delimiter=';')

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
        ''' 
        This function adds some information about the people and institutions involved in the project
        Added features:
        - Number of institutions involved
        - List of institutions involved
        - Coordinator name (if role info is reliable)
        '''
        print('Enriching the projects dataset with people and institutions information.')
        # Count unique institutions per project
        orgs = self.organization_df.groupby('projectID')['name'].nunique().reset_index(name='n_institutions')

        # List all institutions
        inst_list = self.organization_df.groupby('projectID')['name'].apply(list).reset_index(name='institutions')

        # Coordinator (if role info is reliable)
        coordinators = self.organization_df[self.organization_df['role'].str.lower() == 'coordinator']
        pi_names = coordinators.groupby('projectID')['name'].first().reset_index(name='coordinator_name')

        # Merge all
        self.project_df = self.project_df.merge(orgs, how='left', left_on='id', right_on='projectID')
        self.project_df = self.project_df.merge(inst_list, how='left', left_on='id', right_on='projectID')
        self.project_df = self.project_df.merge(pi_names, how='left', left_on='id', right_on='projectID')

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
        ''' 
        Adds scientific and thematic information to the project dataframe:
        - List of full euroSciVocPath strings per project
        - List of euroSciVocTitle values per project
        - List of topic titles from the topics.csv file
        '''
        print('Enriching the projects dataset with thematic / scientific information.')
        # Full paths (euroSciVocPath) per project
        sci_paths = self.sci_voc_df.groupby('projectID')['euroSciVocPath'].apply(list).reset_index(name='sci_voc_paths')

        # Titles per project
        sci_titles = self.sci_voc_df.groupby('projectID')['euroSciVocTitle'].apply(list).reset_index(name='sci_voc_titles')

        # Topics from topics.csv
        topic_titles = self.topics_df.groupby('projectID')['title'].apply(list).reset_index(name='topic_titles')

        # Merge into project_df 
        self.project_df = self.project_df.drop(columns=['projectID']).merge(sci_titles, how='left', left_on='id', right_on='projectID')
        self.project_df = self.project_df.drop(columns=['projectID']).merge(sci_paths, how='left', left_on='id', right_on='projectID')
        self.project_df = self.project_df.drop(columns=['projectID']).merge(topic_titles, how='left', left_on='id', right_on='projectID')


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


class Project_data(CORDIS_data):
    """
    This class is dsed to extract all information related to a certain project from the given datasets.
    It inherits from the CORDIS_data class and its attributes.
    
    Class is initialized with project id or project acronym. 

    TODO:
    - add method to 
    
    """

    def __init__(self, project_id=None, acronym=None):
        # Inherit from CORDIS_data by initialzing parent class
        super().__init__()

        # Check if both project_id and acronym are provided
        self.id, self.acronym = self._resolve_project_id_acronym(project_id, acronym)

        # Add all project-specific data as attributes
        
        self.project_info = self._get_project_info()
        self.publications = self._get_publications()
        self.deliverables = self._get_deliverables()
        self.organizations = self._get_organizations()
        self.scivoc = self._get_scivoc()
        self.topics = self._get_topics()
        self.legal_basis = self._get_legal_basis()

        # Add some enriched data
        self.temporal_features = self._compute_temporal_features()
        self.people_institutions = self._compute_people_institutions()
        self.financial_metrics = self._compute_financial_metrics()
        self.scientific_thematic = self._compute_scientific_thematic()

    def _resolve_project_id_acronym(self, project_id, acronym):
        if project_id is not None and acronym is not None:
            expected_acronym = self.project_df[self.project_df['id'] == project_id]['acronym'].values[0]
            if expected_acronym != acronym:
                raise ValueError(f"Acronym mismatch: ID {project_id} is linked to {expected_acronym}, not {acronym}.")
        elif acronym is not None:
            project_id = self.project_df[self.project_df['acronym'] == acronym]['id'].values[0]
        elif project_id is not None:
            acronym = self.project_df[self.project_df['id'] == project_id]['acronym'].values[0]
        else:
            raise ValueError("Provide at least one of project_id or acronym.")
        return project_id, acronym

    def _get_project_info(self):
        return self.project_df[self.project_df['id'] == self.id].iloc[0].to_dict()

    def _get_publications(self):
        return self.data_publications[self.data_publications['projectID'] == self.id]

    def _get_deliverables(self):
        return self.data_deliverables[self.data_deliverables['projectID'] == self.id]

    def _get_organizations(self):
        return self.organization_df[self.organization_df['projectID'] == self.id]

    def _get_scivoc(self):
        return self.sci_voc_df[self.sci_voc_df['projectID'] == self.id]

    def _get_topics(self):
        return self.topics_df[self.topics_df['projectID'] == self.id]

    def _get_legal_basis(self):
        return self.legal_basis_df[self.legal_basis_df['projectID'] == self.id]
    

    # Add additional project features
    def _compute_temporal_features(self):
        fmt = "%Y-%m-%d"
        start = self.project_info.get("startDate", None)
        end = self.project_info.get("endDate", None)
        ec_sig = self.project_info.get("ecSignatureDate", None)

        try:
            start_date = datetime.strptime(start, fmt)
            end_date = datetime.strptime(end, fmt)
            duration_days = (end_date - start_date).days
        except:
            duration_days = None

        return {
            "start_year": start.year if start else None,
            "end_year": end.year if end else None,
            "signature_year": ec_sig[:4] if ec_sig else None,
            "duration_days": duration_days
        }
    
    def _compute_people_institutions(self):
        orgs = self.organizations
        if orgs.empty:
            return {}
        country_counts = orgs["country"].value_counts().to_dict()
        activity_types = orgs["activityType"].value_counts().to_dict()
        n_partners = orgs["organisationID"].nunique()

        return {
            "n_partners": n_partners,
            "countries": country_counts,
            "activity_types": activity_types
        }

    def _compute_financial_metrics(self):
        ec_total = self.project_info.get("ecMaxContribution", None)
        total_cost = self.project_info.get("totalCost", None)
        ec_partner_sum = self.organizations["ecContribution"].sum()
        cost_partner_sum = self.organizations["totalCost"].sum()

        try:
            ec_per_deliverable = ec_total / len(self.deliverables)
        except:
            ec_per_deliverable = None

        try:
            ec_per_publication = ec_total / len(self.publications)
        except:
            ec_per_publication = None

        return {
            "ec_total": ec_total,
            "total_cost": total_cost,
            "ec_sum_from_partners": ec_partner_sum,
            "cost_sum_from_partners": cost_partner_sum,
            "ec_per_deliverable": ec_per_deliverable,
            "ec_per_publication": ec_per_publication
        }

    def _compute_scientific_thematic(self):
        scivoc_titles = self.scivoc['euroSciVocTitle'].dropna().unique().tolist()
        topic_titles = self.topics['title'].dropna().unique().tolist()

        pub_types = self.publications['isPublishedAs'].value_counts().to_dict()
        deliverable_types = self.deliverables['deliverableType'].value_counts().to_dict()

        return {
            "scivoc_keywords": scivoc_titles,
            "topic_keywords": topic_titles,
            "publication_types": pub_types,
            "deliverable_types": deliverable_types
        }
    def summary(self):
        return {
            "project_id": self.id,
            "acronym": self.acronym,
            "title": self.project_info.get("title", ""),
            "temporal": self.temporal_features,
            "institutions": self.people_institutions,
            "financials": self.financial_metrics,
            "keywords": self.scientific_thematic
        }
    
    def inspect_project_data(self):
        """
        Print or return a structured overview of all enriched data for the selected project.
        """
        if not hasattr(self, 'id') or not hasattr(self, 'acronym'):
            raise AttributeError("Please set a project using the `project()` method first.")

        print(f"\nProject: {self.acronym} (ID: {self.id})")
        print("="*60)

        print("\nPublications:")
        pprint(getattr(self, 'publications', {}), indent=4)

        print("\nDeliverables:")
        pprint(getattr(self, 'deliverables', {})[['deliverableType', 'description']], indent=4)

        print("\nInstitutions / Organizations:")
        pprint(getattr(self, 'organizations', []), indent=4)

        print("\nFinancial Info:")
        pprint({
            'Total Cost': getattr(self, 'total_cost', None),
            'EC Contribution': getattr(self, 'ec_contribution', None),
            'Num Orgs': getattr(self, 'num_organizations', None),
            'Countries': getattr(self, 'countries', None),
        }, indent=4)

        print("\nDates & Duration:")
        pprint({
            'Start Date': getattr(self, 'start_date', None),
            'End Date': getattr(self, 'end_date', None),
            'Duration (days)': getattr(self, 'duration_days', None),
            'Year': getattr(self, 'year', None)
        }, indent=4)

        print("\nLegal & Administrative:")
        pprint({
            'Legal Basis': getattr(self, 'legal_basis', None),
            'Funding Scheme': getattr(self, 'funding_scheme', None),
            'Framework Programme': getattr(self, 'framework_programme', None),
        }, indent=4)

        print("\n🔬 Scientific Keywords (euroSciVoc):")
        pprint(getattr(self, 'sci_keywords', []), indent=4)

        print("\nProject Topics:")
        pprint(getattr(self, 'topics', []), indent=4)

        print("\nWeb Links:")
        pprint(getattr(self, 'web_links', []), indent=4)
