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
        else:
            P = self.processed_dir
            # core
            self.project_df         = pd.read_csv(f'{P}/projects.csv')
            self.data_deliverables  = pd.read_csv(f'{P}/deliverables.csv')
            self.data_publications  = pd.read_csv(f'{P}/publications.csv')
            # dimensions
            self.organization_df    = pd.read_csv(f'{P}/organizations.csv')
            self.topics_df          = pd.read_csv(f'{P}/topics.csv')
            self.legal_basis_df     = pd.read_csv(f'{P}/legal_basis.csv')
            self.sci_voc_df         = pd.read_csv(f'{P}/sci_voc.csv')
            # joins
            self.project_organizations = pd.read_csv(f'{P}/project_organizations.csv')
            self.project_topics        = pd.read_csv(f'{P}/project_topics.csv')
            self.project_legal_basis   = pd.read_csv(f'{P}/project_legal_basis.csv')
            self.project_sci_voc       = pd.read_csv(f'{P}/project_sci_voc.csv')
            self.web_items_df       = pd.read_csv(f'{P}/web_items.csv')
            self.web_link_df        = pd.read_csv(f'{P}/web_links.csv')
            

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
        paths = self.sci_voc_df['path'].dropna().unique()
        fields = set()
        for path in paths:
            segments = path.strip('/').split('/')
            if segments:
                fields.add(segments[0])
        return sorted(fields)
    
    
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

    def export_to_db_schema(self, directory):
        """
        Export all enriched tables to match the final database schema.
        Produces CSVs:
        - projects.csv
        - topics.csv
        - project_topics.csv
        - legal_basis.csv
        - project_legal_basis.csv
        - organizations.csv
        - project_organizations.csv
        - deliverables.csv
        - publications.csv
        - sci_voc.csv
        - project_sci_voc.csv
        - web_items.csv
        - web_links.csv
        """
        out = directory
        os.makedirs(out, exist_ok=True)

        # 1) projects
        proj = self.project_df.copy()
        print("Exporting projects to CSV...")
        print(f"  - {len(proj)} projects found")
        #print columns of the project dataframe
        print(f"  - Columns: {', '.join(proj.columns)}")
        proj = proj.rename(columns={
            'startDate': 'start_date',
            'endDate': 'end_date',
            'totalCost': 'total_cost',
            'ecMaxContribution': 'ec_max_contribution',
            'ecSignatureDate': 'ec_signature_date',
            'contentUpdateDate': 'content_update_date',
            'grantDoi': 'grant_doi',
            'frameworkProgramme': 'framework_programme',
            'masterCall': 'master_call',
            'subCall': 'sub_call',
            'fundingScheme': 'funding_scheme',
            'nature': 'nature',
            'objective': 'objective',
            'rcn': 'rcn',
            'grantDoi': 'grant_doi',
            'ecContribution_per_year': 'ec_contribution_per_year',
            'totalCost_per_year': 'total_cost_per_year',
            'subfield' : 'sub_field',
            
        })
        keep = [
            'id', 'acronym', 'status', 'title',
            'start_date', 'end_date', 'total_cost', 'ec_max_contribution', 'ec_signature_date',
            'framework_programme', 'master_call', 'sub_call', 'funding_scheme', 'nature', 'objective', 'content_update_date',
            'rcn', 'grant_doi',
            'duration_days', 'duration_months', 'duration_years',
            'n_institutions', 'coordinator_name',
            'ec_contribution_per_year', 'total_cost_per_year',
            'field_class', 'field', 'sub_field', 'niche',
        ]
        proj[keep].to_csv(os.path.join(out, 'projects.csv'), index=False)

        # 2) topics & project_topics
        topics = self.topics_df.rename(columns={'projectID':'project_id', 'topic':'code'})
        dim_topics = topics[['code','title']].drop_duplicates()
        dim_topics.to_csv(os.path.join(out, 'topics.csv'), index=False)
        proj_topics = topics[['project_id','code']].drop_duplicates()
        # rename code to topic_code
        proj_topics = proj_topics.rename(columns={'code':'topic_code'})
        proj_topics.to_csv(os.path.join(out, 'project_topics.csv'), index=False)

        # 3) legal_basis & project_legal_basis
        lb = self.legal_basis_df.rename(columns={'projectID':'project_id','legalBasis':'code'})
        dim_lb = lb[['code','title','uniqueProgrammePart']].drop_duplicates()
        # rename uniqueProgrammePart to unique_programme_part
        dim_lb = dim_lb.rename(columns={'uniqueProgrammePart':'unique_programme_part'})
        
        dim_lb.to_csv(os.path.join(out, 'legal_basis.csv'), index=False)
        
        
        proj_lb = lb[['project_id','code']].drop_duplicates()
        # rename code to legal_basis_code
        proj_lb = proj_lb.rename(columns={'code':'legal_basis_code'})
        proj_lb.to_csv(os.path.join(out, 'project_legal_basis.csv'), index=False)

        # 4) organizations & project_organizations
        print("Exporting organizations and project_organizations to CSV...")
        print(f"  - {len(self.organization_df)} organizations found")
        print(f"  - Columns: {', '.join(self.organization_df.columns)}")
        
        # 4) organizations & project_organizations
        org = self.organization_df.rename(columns={
            'organisationID':'id',
            'projectID':'project_id',
            'SME':'sme',
            'shortName':'short_name',
            'vatNumber':'vat_number',
            'activityType':'activity_type',
            'street':'street',
            'postCode':'post_code',
            'city':'city',
            'country':'country',
            'nutsCode':'nuts_code',
            'geolocation':'geolocation',
            'organizationURL':'organization_url',
            'contactForm':'contact_form',
            'contentUpdateDate':'content_update_date',
            'grantDoi':'grant_doi',
        })
        dim_org = org[['id','name','short_name','vat_number','sme','activity_type',
                       'street','post_code','city','country','nuts_code','geolocation',
                       'organization_url','contact_form','content_update_date']].drop_duplicates()
        dim_org.to_csv(os.path.join(out, 'organizations.csv'), index=False)

        proj_org = org.rename(columns={
            'order':'order_index',
            'ecContribution':'ec_contribution',
            'netEcContribution':'net_ec_contribution',
            'totalCost':'total_cost',
            'endOfParticipation':'end_of_participation'
        })
        link_cols = ['project_id','id','role','order_index','ec_contribution','net_ec_contribution',
                     'total_cost','end_of_participation','active']
        proj_org = proj_org[link_cols].rename(columns={'id':'organization_id'})
        proj_org.to_csv(os.path.join(out, 'project_organizations.csv'), index=False)
        
        
        
        # 5) deliverables
        print("Exporting deliverables to CSV...")
        print(f"  - {len(self.data_deliverables)} deliverables found")
        print(f"  - Columns: {', '.join(self.data_deliverables.columns)}")
        # Rename columns and select relevant ones
        
        
        deliv = self.data_deliverables.rename(columns={
            'projectID':'project_id',
            'deliverableID':'id',
            'deliverableType':'deliverable_type',
            'contentUpdateDate':'content_update_date',
            'contentupdatedate':'content_update_date'
        })
        deliv_cols = ['id','project_id','title','deliverable_type','description','url','collection','content_update_date']
        deliv[deliv_cols].to_csv(os.path.join(out, 'deliverables.csv'), index=False)



        # 6) publications
        pubs = self.data_publications.rename(columns={
            'projectID':'project_id','publicationID':'id','isPublishedAs':'is_published_as',
            'journalTitle':'journal_title','journalNumber':'journal_number',
            'publishedYear':'published_year','publishedPages':'published_pages',
            'contentUpdateDate':'content_update_date'
        })
        pub_cols = ['id','project_id','title','is_published_as','authors','journal_title','journal_number',
                    'published_year','published_pages','issn','isbn','doi','collection','content_update_date']
        pubs[pub_cols].to_csv(os.path.join(out, 'publications.csv'), index=False)

        # 7) sci_voc & project_sci_voc
        sci = self.sci_voc_df.rename(columns={
            'euroSciVocCode':'code','euroSciVocPath':'path',
            'euroSciVocTitle':'title','euroSciVocDescription':'description'
        })
        dim_sci = sci[['code','path','title','description']].drop_duplicates()
        dim_sci.to_csv(os.path.join(out, 'sci_voc.csv'), index=False)
        proj_sci = sci.rename(columns={'projectID':'project_id'})[['project_id','code']].drop_duplicates()
        # rename code to sci_voc_code
        proj_sci = proj_sci.rename(columns={'code':'sci_voc_code'})
        proj_sci.to_csv(os.path.join(out, 'project_sci_voc.csv'), index=False)

        # 8) web_items
        print("Exporting web items to CSV...")
        print(f"  - {len(self.web_items_df)} web items found")
        print(f"  - Columns: {', '.join(self.web_items_df.columns)}")
        # Rename columns and select relevant ones
        wi = self.web_items_df.rename(columns={'represents':'project_id','availableLanguages':'available_languages'})
        wi[['language','available_languages','uri','title','type','source','project_id']].to_csv(os.path.join(out, 'web_items.csv'), index=False)

        # 9) web_links
        print("Exporting web links to CSV...")
        print(f"  - {len(self.web_link_df)} web links found")
        print(f"  - Columns: {', '.join(self.web_link_df.columns)}")
        wl = self.web_link_df.rename(columns={
            'projectID':'project_id',
            'physUrl':'phys_url',
            'availableLanguages':'available_languages',
            'archivedDate':'archived_date'
        })
        web_link_cols = ['id','project_id','phys_url','available_languages','status','archived_date','type','source','represents']
        wl[web_link_cols].to_csv(os.path.join(out, 'web_links.csv'), index=False)

        print(f"✅ All tables exported to {out}")

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
            _save(self.data_deliverables, "data_deliverables_v2")
            _save(self.data_publications, "data_publications_v2")
            _save(self.organization_df, "organization_df_v2")
            _save(self.legal_basis_df, "legal_basis_df_v2")
            _save(self.topics_df, "topics_df_v2")
            _save(self.sci_voc_df, "sci_voc_df_v2")
            _save(self.web_items_df, "web_items_df_v2")
            _save(self.web_link_df, "web_link_df_v2")