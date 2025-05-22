"""
Python file containing the Project_data class. 
These are used to load an process the data from the CSV files in the CORDIS framework.


Project_data is a subclass of CORDIS_data and is used to extract all inormation related to a single project,
based on the project identifier or acronym. Also, this extracts some additional information from the dataset by 
processing infromation given in the supplemantry CORDIS datasets such as euroSciVoc, legal basis, etc. 
"""


# class to load datasets
# Imports 
from datetime import datetime
from pprint import pprint


from .cordis_data import CORDIS_data

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
