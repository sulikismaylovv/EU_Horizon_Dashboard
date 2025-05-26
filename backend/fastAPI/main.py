from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from pydantic import BaseModel, validator, Field
from typing import Optional, List, Dict, Any
from datetime import date, datetime
import pandas as pd # Import pandas

# Load environment variables from .env file
load_dotenv()

# Supabase credentials
SUPABASE_URL = "https://nmkhssxsltmufkkgcfcu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5ta2hzc3hzbHRtdWZra2djZmN1Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0ODAzNDM3NSwiZXhwIjoyMDYzNjEwMzc1fQ.7vzQzb1blpQ5oXAy8XZAoJzwLhN1yMQZ9VuDOEljkuM"


# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize FastAPI app
app = FastAPI(
    title="EU Horizon Projects API",
    description="API to fetch project data and analytics from Supabase.",
    version="1.0.0"
)
origins = [
    "http://localhost:3000",  # Local development
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Specifies the origins that are allowed to make requests.
    allow_credentials=True, # Indicates that cookies should be supported for cross-origin requests.
    allow_methods=["*"],    # Allows all common HTTP methods (GET, POST, PUT, DELETE, etc.).
                            # You can specify methods: ["GET", "POST"]
    allow_headers=["*"],    # Allows all headers. You can specify specific headers if needed.
)


# --- Pydantic Models ---
class ProjectOrganizationBase(BaseModel):

    project_id: int
    organization_id: int
    role: Optional[str] = None
    order_index: Optional[int] = None
    ec_contribution: Optional[float] = None
    net_ec_contribution: Optional[float] = None
    total_cost: Optional[float] = None
    end_of_participation: Optional[bool] = None
    active: Optional[bool] = None


class Project(BaseModel):
    id: int
    acronym: str
    status: Optional[str] = None
    title: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    total_cost: Optional[float] = None
    ec_max_contribution: Optional[float] = None
    ec_signature_date: Optional[date] = None
    framework_programme: Optional[str] = None
    master_call: Optional[str] = None
    sub_call: Optional[str] = None
    funding_scheme: Optional[str] = None
    nature: Optional[str] = None
    objective: Optional[str] = None
    content_update_date: Optional[datetime] = None
    rcn: Optional[str] = None
    grant_doi: Optional[str] = None
    duration_days: Optional[int] = None
    duration_months: Optional[int] = None
    duration_years: Optional[int] = None
    n_institutions: Optional[int] = None
    coordinator_name: Optional[str] = None
    ec_contribution_per_year: Optional[float] = None
    total_cost_per_year: Optional[float] = None
    field_class: Optional[str] = None
    field: Optional[str] = None
    sub_field: Optional[str] = None
    niche: Optional[str] = None

    @validator('total_cost', 'ec_max_contribution', 'ec_contribution_per_year', 'total_cost_per_year', pre=True)
    def parse_numeric(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None # Or raise an error, or handle as appropriate
        return value

class SciVoc(BaseModel): # For data from sci_voc table
    code: str
    path: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None

class SunburstChartData(BaseModel):
    labels: List[str]
    parents: List[str]
    values: List[Optional[float]] # Values for the selected metric
    metric_name: str = Field(..., description="The metric used for the 'values' field.")
    max_level_processed: int = Field(..., description="Number of path levels processed for the hierarchy (0-indexed).")



class ProjectAnalytics(BaseModel):
    total_project_count: int = Field(..., description="Total number of projects.")
    projects_by_status: Dict[str, int] = Field(..., description="Count of projects grouped by status.")
    average_total_cost: Optional[float] = Field(None, description="Average total cost of projects.")
    projects_per_framework: Dict[str, int] = Field(..., description="Count of projects per framework programme.")
    data_last_updated: Optional[datetime] = Field(None, description="Timestamp of the most recent content_update_date among projects.")

class TimeSeriesData(BaseModel):
    year: int
    metric_value: float
    metric_name: str

class ProjectTimelineResponse(BaseModel):
    chart_title: str
    x_axis_label: str = "Year"
    y_axis_label: str
    data: List[TimeSeriesData]
    chart_type: str = "line"

class NetworkNode(BaseModel):
    id: str
    label: str
    size: float
    group: Optional[str] = None

class NetworkEdge(BaseModel):
    source: str
    target: str
    weight: float

class NetworkGraphResponse(BaseModel):
    chart_title: str
    nodes: List[NetworkNode]
    edges: List[NetworkEdge]
    description: str

class DistributionData(BaseModel):
    label: str
    value: float
    count: int

class DistributionResponse(BaseModel):
    chart_title: str
    data: List[DistributionData]
    chart_type: str
    x_axis_label: str
    y_axis_label: str

class HeatmapData(BaseModel):
    x_category: str
    y_category: str
    value: float

class HeatmapResponse(BaseModel):
    chart_title: str
    data: List[HeatmapData]
    x_axis_label: str
    y_axis_label: str
    color_scale_label: str

class BubbleChartData(BaseModel):
    x: float
    y: float
    size: float
    label: str
    category: Optional[str] = None

class BubbleChartResponse(BaseModel):
    chart_title: str
    data: List[BubbleChartData]
    x_axis_label: str
    y_axis_label: str
    size_label: str

class PublicationAnalysis(BaseModel):
    year: int
    publication_count: int
    projects_with_publications: int
    avg_publications_per_project: float

class PublicationTimelineResponse(BaseModel):
    chart_title: str = "Research Output Timeline"
    x_axis_label: str = "Year"
    y_axis_label: str = "Count"
    data: List[PublicationAnalysis]

class TopicEvolutionData(BaseModel):
    topic: str
    year: int
    project_count: int
    funding_amount: float

class TopicEvolutionResponse(BaseModel):
    chart_title: str = "Topic Evolution Over Time"
    data: List[TopicEvolutionData]
    x_axis_label: str = "Year"
    y_axis_label: str = "Project Count"

class ProjectSuccessMetrics(BaseModel):
    project_acronym: str
    duration_months: int
    publications_count: int
    funding_efficiency: float  # publications per million EUR
    collaboration_score: int  # number of participating countries

class ProjectSuccessResponse(BaseModel):
    chart_title: str = "Project Success Analysis"
    data: List[ProjectSuccessMetrics]
    x_axis_label: str = "Publications Count"
    y_axis_label: str = "Funding Efficiency"

class ResearchFieldNetworkNode(BaseModel):
    id: str
    label: str
    size: float
    publications: int
    funding: float

class ResearchFieldNetworkResponse(BaseModel):
    chart_title: str = "Research Field Collaboration Network"
    nodes: List[ResearchFieldNetworkNode]
    edges: List[NetworkEdge]

class SeasonalityData(BaseModel):
    month: int
    month_name: str
    project_starts: int
    avg_funding: float

class SeasonalityResponse(BaseModel):
    chart_title: str = "Project Start Seasonality"
    data: List[SeasonalityData]
    chart_type: str = "polar"

# --- FastAPI Endpoints ---

@app.get("/")
async def read_root():
    return {"message": "Hello from FastAPI! CORS is enabled."}


@app.get("/projects", response_model=List[Project], tags=["Projects"])
async def get_projects():
    """
    Fetches all projects from the Supabase 'projects' table.
    """
    try:
        response = supabase.table("projects").select("*").execute()
        projects_data = response.data
        if projects_data is None: # Handle case where data might be None from Supabase
             return []
        return projects_data
    except Exception as e:
        print(f"Error fetching projects: {e}") # Log the error
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching projects: {str(e)}")

@app.get("/project/{project_id}", response_model=Project, tags=["Projects"])
async def get_project_by_id(project_id: int):
    """
    Fetches a single project by its ID from the Supabase 'projects' table.
    """
    try:
        response = supabase.table("projects").select("*").eq("id", project_id).single().execute()
        project_data = response.data
        if not project_data:
            raise HTTPException(status_code=404, detail="Project not found")
        return project_data
    except Exception as e:
        # Check if the error is due to "PGRST116" (resource not found from PostgREST)
        if hasattr(e, 'message') and "PGRST116" in e.message:
             raise HTTPException(status_code=404, detail="Project not found (PGRST116)")
        print(f"Error fetching project by ID {project_id}: {e}") # Log the error
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/projects/analytics", response_model=ProjectAnalytics, tags=["Analytics"])
async def get_project_analytics():
    """
    Fetches all projects, performs basic analytics using Pandas, and returns the results.
    """
    try:
        # 1. Fetch all project data
        projects_response = supabase.table("projects").select("status, total_cost, framework_programme, content_update_date").execute()
        projects_data = projects_response.data

        if not projects_data:
            # Return default analytics if no data, or raise an error
            return ProjectAnalytics(
                total_project_count=0,
                projects_by_status={},
                average_total_cost=None,
                projects_per_framework={},
                data_last_updated=None
            )

        # 2. Convert to Pandas DataFrame
        # Ensure that 'total_cost' is numeric. If it can be None or non-numeric string, handle it.
        for project in projects_data:
            cost_str = project.get('total_cost')
            if isinstance(cost_str, str):
                try:
                    project['total_cost'] = float(cost_str)
                except (ValueError, TypeError):
                    project['total_cost'] = None # Or pd.NA or 0, depending on how you want to treat invalid/missing costs
            elif not isinstance(cost_str, (int, float)):
                 project['total_cost'] = None


        df = pd.DataFrame(projects_data)

        # 3. Perform Analytics
        total_project_count = len(df)

        # Projects by status (handle potential None values in status)
        df['status'] = df['status'].fillna('Unknown') # Replace None status with 'Unknown'
        status_counts = df['status'].value_counts()
        # Explicitly convert keys to str and values to int for Pydantic validation
        projects_by_status = {str(k): int(v) for k, v in status_counts.items()}


        # Average total_cost (ensure 'total_cost' is numeric and handle NaNs)
        # The Pydantic model validator already tries to convert to float.
        # If conversion failed and resulted in None, those will be ignored by .mean()
        average_total_cost = df['total_cost'].astype(float).mean() # Convert to float explicitly for mean
        if pd.isna(average_total_cost): # Handle case where mean results in NaN (e.g., all costs are None)
            average_total_cost = None

        # Projects per framework programme
        df['framework_programme'] = df['framework_programme'].fillna('Not Specified')
        framework_counts = df['framework_programme'].value_counts()
        # Explicitly convert keys to str and values to int for Pydantic validation
        projects_per_framework = {str(k): int(v) for k, v in framework_counts.items()}


        # Data last updated (most recent content_update_date)
        # Convert 'content_update_date' to datetime objects, coercing errors to NaT
        df['content_update_date'] = pd.to_datetime(df['content_update_date'], errors='coerce')
        data_last_updated_ts = df['content_update_date'].max() # Keep original pd.Timestamp or NaT
        # Convert Timestamp to datetime if it's not NaT, else None
        if pd.isna(data_last_updated_ts):
            data_last_updated = None
        else:
            data_last_updated = data_last_updated_ts.to_pydatetime()


        # 4. Structure and return results
        analytics_results = ProjectAnalytics(
            total_project_count=total_project_count,
            projects_by_status=projects_by_status,
            average_total_cost=average_total_cost,
            projects_per_framework=projects_per_framework,
            data_last_updated=data_last_updated
        )
        return analytics_results

    except Exception as e:
        print(f"Error performing project analytics: {e}") # Log the error
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        raise HTTPException(status_code=500, detail=f"An error occurred during analytics: {str(e)}")

METRICS_LIST_SUNBURST = [
    'total_cost', 'ec_max_contribution', 'total_cost_per_year',
    'ec_contribution_per_year', 'duration_days', 'duration_months',
    'duration_years', 'n_institutions'
]
MAX_SUNBURST_LEVEL = 4 # Corresponds to path_lvl_0 to path_lvl_4

class EcByCountryData(BaseModel):
    country: str
    ec_contribution: Optional[float] = None

# Ensure Query is imported:
# from fastapi import FastAPI, HTTPException, Query, ...
# ... other imports ...
# import pandas as pd

# Add this Pydantic model with your other models
class InstitutionFunding(BaseModel):
    name: str
    ec_contribution: Optional[float] = None

    @validator('ec_contribution', pre=True)
    def parse_numeric_ec_contribution(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None # Or raise an error, or handle as appropriate
        return value

# Add this new endpoint function with your other FastAPI endpoints
@app.get("/analytics/top-institutions-by-funding", response_model=List[InstitutionFunding], tags=["Analytics"])
async def get_top_institutions_by_funding(
    top_n: int = Query(15, gt=0, description="The number of top institutions to return.")
):
    """
    Calculates and returns the top N institutions by total EC contribution.
    """
    try:
        # 1. Fetch project_organizations data (organization_id, ec_contribution)
        po_response = supabase.table("project_organizations").select("organization_id, ec_contribution").execute()
        po_data = po_response.data
        if not po_data:
            po_data = []

        # 2. Fetch organizations data (id, name)
        # Assuming 'organizations' table has 'id' and 'name' columns
        org_response = supabase.table("organizations").select("id, name").execute() # 'name' is the institution name
        org_data = org_response.data
        if not org_data:
            org_data = []

        if not po_data: # If there's no project_organization data, no contributions to aggregate
            return []

        # Convert to Pandas DataFrames
        po_df = pd.DataFrame(po_data)
        org_df = pd.DataFrame(org_data)

        # Ensure 'ec_contribution' is numeric and handle potential errors/None values
        if 'ec_contribution' in po_df.columns:
            po_df['ec_contribution'] = pd.to_numeric(po_df['ec_contribution'], errors='coerce').fillna(0.0)
        else:
            po_df['ec_contribution'] = 0.0 # If column is missing, contributions are zero

        if org_df.empty: # If no organization data, we can't map to institution names
            if not po_df.empty:
                # Sum all EC contributions under 'Unknown Institution'
                total_ec_unknown = po_df['ec_contribution'].sum()
                if total_ec_unknown > 0: # Only return if there's some contribution
                     return [{"name": "Unknown Institution", "ec_contribution": total_ec_unknown}]
            return []
        
        # Ensure org_df has 'id' and 'name' columns
        if 'id' not in org_df.columns or 'name' not in org_df.columns:
            # Essential columns missing from organizations table for this logic
             if not po_df.empty:
                total_ec_unknown = po_df['ec_contribution'].sum()
                if total_ec_unknown > 0:
                     return [{"name": "Unknown Institution (Org data missing)", "ec_contribution": total_ec_unknown}]
             return []


        # 3. Merge dataframes
        merged_df = pd.merge(
            po_df,
            org_df,
            left_on='organization_id',
            right_on='id',
            how='left'
        )

        # Handle cases where institution name might be NaN (e.g., organization not found or name is null)
        if 'name' in merged_df.columns:
            merged_df['name'] = merged_df['name'].fillna('Unknown Institution')
        else:
            # This case should ideally be caught by the org_df check above,
            # but as a fallback:
            merged_df['name'] = 'Unknown Institution'

        # 4. Group by institution name and sum ec_contribution
        institution_funding_df = merged_df.groupby('name', as_index=False)['ec_contribution'].sum()

        # 5. Sort values by ec_contribution in descending order and take top N
        institution_funding_df = institution_funding_df.sort_values('ec_contribution', ascending=False).head(top_n)

        # 6. Convert DataFrame to list of dictionaries
        result = institution_funding_df.to_dict(orient='records')
        
        return result

    except Exception as e:
        print(f"Error calculating top institutions by funding: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred calculating top institutions by funding: {str(e)}")

# Add this Pydantic model with your other models
class ProjectsByCountryData(BaseModel):
    country: str
    project_count: int

# Add this new endpoint function with your other FastAPI endpoints
@app.get("/analytics/projects-by-country", response_model=List[ProjectsByCountryData], tags=["Analytics"])
async def get_projects_per_country():
    """
    Calculates and returns the number of unique projects per country.
    This data is suitable for a bar chart.
    """
    try:
        # 1. Fetch project_organizations data (only necessary columns)
        po_response = supabase.table("project_organizations").select("project_id, organization_id, ec_contribution").execute() # ec_contribution not strictly needed here but often fetched together
        po_data = po_response.data
        if not po_data:
            po_data = []

        # 2. Fetch organizations data (only necessary columns: id and country)
        org_response = supabase.table("organizations").select("id, country").execute()
        org_data = org_response.data
        if not org_data:
            org_data = []

        if not po_data: # If there's no project_organization data, no projects to count per country
            return []

        # Convert to Pandas DataFrames
        po_df = pd.DataFrame(po_data)
        org_df = pd.DataFrame(org_data)
        
        # Ensure po_df has 'project_id' and 'organization_id'
        if 'project_id' not in po_df.columns or 'organization_id' not in po_df.columns:
            # This case means essential data is missing from project_organizations fetch
            # or the table itself.
            return []


        if org_df.empty: # If no organization data, we can't map to countries
            if not po_df.empty and 'project_id' in po_df.columns:
                # Count all unique projects under 'Unknown' country
                project_count_unknown_country = po_df['project_id'].nunique()
                return [{"country": "Unknown", "project_count": project_count_unknown_country}]
            return []

        # 3. Merge dataframes
        # project_organizations.organization_id links to organizations.id
        merged_df = pd.merge(
            po_df,
            org_df,
            left_on='organization_id',
            right_on='id',
            how='left'
        )

        # Handle cases where country might be NaN
        if 'country' in merged_df.columns:
            merged_df['country'] = merged_df['country'].fillna('Unknown')
        else:
            merged_df['country'] = 'Unknown' # If 'country' column missing from org_df or merge

        # 4. Group by country and count unique project_ids
        # The .nunique() method on a grouped series returns a series,
        # so we reset_index to turn it into a DataFrame.
        # Or use as_index=False in groupby and then rename.
        projects_by_country_df = merged_df.groupby('country', as_index=False)['project_id'].nunique()
        
        # 5. Rename the column containing unique project counts
        projects_by_country_df = projects_by_country_df.rename(columns={'project_id': 'project_count'})

        # 6. Sort values by project_count in descending order
        projects_by_country_df = projects_by_country_df.sort_values('project_count', ascending=False)

        # 7. Convert DataFrame to list of dictionaries
        result = projects_by_country_df.to_dict(orient='records')
        
        return result

    except Exception as e:
        print(f"Error calculating projects per country: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred calculating projects per country: {str(e)}")

@app.get("/analytics/ec-by-country", response_model=List[EcByCountryData], tags=["Analytics"])
async def get_ec_contribution_by_country():
    """
    Calculates and returns the total EC contribution by country.
    This data is suitable for a bar chart.
    """
    try:
        # 1. Fetch project_organizations data (only necessary columns)
        po_response = supabase.table("project_organizations").select("organization_id, ec_contribution").execute()
        po_data = po_response.data
        if not po_data:
            po_data = [] # Ensure po_df can be created if no data

        # 2. Fetch organizations data (only necessary columns: id and country)
        # Assuming 'organizations' table has 'id' and 'country' columns
        org_response = supabase.table("organizations").select("id, country").execute()
        org_data = org_response.data
        if not org_data:
            org_data = [] # Ensure org_df can be created if no data

        if not po_data: # If there's no project_organization data, no contributions to aggregate
            return []

        # Convert to Pandas DataFrames
        po_df = pd.DataFrame(po_data)
        org_df = pd.DataFrame(org_data)

        # Ensure 'ec_contribution' is numeric and handle potential errors/None values
        if 'ec_contribution' in po_df.columns:
            po_df['ec_contribution'] = pd.to_numeric(po_df['ec_contribution'], errors='coerce').fillna(0.0)
        else:
            # If 'ec_contribution' column doesn't exist, treat all contributions as 0
            po_df['ec_contribution'] = 0.0
        
        if org_df.empty: # If no organization data, we can't map to countries
            # Option 1: Return sum under 'Unknown' country if po_df has data
            if not po_df.empty:
                total_ec_unknown_country = po_df['ec_contribution'].sum()
                return [{"country": "Unknown", "ec_contribution": total_ec_unknown_country}]
            # Option 2: Return empty list
            return []


        # 3. Merge dataframes
        # project_organizations.organization_id links to organizations.id
        merged_df = pd.merge(
            po_df,
            org_df,
            left_on='organization_id',
            right_on='id',
            how='left'
        )

        # Handle cases where country might be NaN (e.g., organization not found in org_df or country is null)
        if 'country' in merged_df.columns:
            merged_df['country'] = merged_df['country'].fillna('Unknown')
        else:
            # If 'country' column doesn't exist after merge (e.g. org_df was empty or had no 'country' column)
            merged_df['country'] = 'Unknown'


        # 4. Group by country and sum ec_contribution
        ec_by_country_df = merged_df.groupby('country', as_index=False)['ec_contribution'].sum()

        # 5. Sort values by ec_contribution in descending order
        ec_by_country_df = ec_by_country_df.sort_values('ec_contribution', ascending=False)

        # 6. Convert DataFrame to list of dictionaries (Pydantic model will validate this structure)
        result = ec_by_country_df.to_dict(orient='records')
        
        return result

    except Exception as e:
        print(f"Error calculating EC contribution by country: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred calculating EC contribution by country: {str(e)}")


@app.get("/projects/analytics/sunburst", response_model=SunburstChartData, tags=["Analytics"])
async def get_sunburst_data(
    metric: str = Query('ec_max_contribution', enum=METRICS_LIST_SUNBURST, description="Metric to be used for sunburst values.")
):
    """
    Generates data suitable for a sunburst chart, based on project metrics
    and scientific vocabulary (sci_voc) hierarchy.
    """
    try:
        # 1. Fetch data from Supabase
        # Fetch projects (only relevant columns)
        project_cols_to_fetch = ["id"] + METRICS_LIST_SUNBURST
        projects_response = supabase.table("projects").select(",".join(project_cols_to_fetch)).execute()
        proj_df_data = projects_response.data
        if not proj_df_data: proj_df_data = []


        # Fetch sci_voc
        sci_voc_response = supabase.table("sci_voc").select("code, path, title").execute()
        sci_df_data = sci_voc_response.data
        if not sci_df_data: sci_df_data = []

        # Fetch project_sci_voc (links)
        link_response = supabase.table("project_sci_voc").select("project_id, sci_voc_code").execute()
        link_df_data = link_response.data
        if not link_df_data: link_df_data = []

        if not proj_df_data or not sci_df_data or not link_df_data:
            return SunburstChartData(labels=[], parents=[], values=[], metric_name=metric, max_level_processed=MAX_SUNBURST_LEVEL)

        # Data type conversion for project metrics before creating DataFrame
        int_metrics = ['duration_days', 'duration_months', 'duration_years', 'n_institutions']
        float_metrics = ['total_cost', 'ec_max_contribution', 'total_cost_per_year', 'ec_contribution_per_year']

        for project_row in proj_df_data:
            for metric_col in int_metrics:
                val = project_row.get(metric_col)
                if isinstance(val, str):
                    try: project_row[metric_col] = int(float(val)) # float first for "123.0"
                    except (ValueError, TypeError): project_row[metric_col] = None
                elif not isinstance(val, (int, type(None))): project_row[metric_col] = None

            for metric_col in float_metrics:
                val = project_row.get(metric_col)
                if isinstance(val, str):
                    try: project_row[metric_col] = float(val)
                    except (ValueError, TypeError): project_row[metric_col] = None
                elif not isinstance(val, (float, int, type(None))): project_row[metric_col] = None


        proj_df = pd.DataFrame(proj_df_data)
        sci_df = pd.DataFrame(sci_df_data)
        link_df = pd.DataFrame(link_df_data)

        # 2. Build a single DataFrame (replicating notebook logic)
        df = (
            link_df
            .merge(sci_df.rename(columns={'code': 'sci_voc_code',
                                          'path': 'sci_voc_path',
                                          'title': 'sci_voc_title'}),
                   on='sci_voc_code',
                   how='left')
            .merge(proj_df.rename(columns={'id': 'project_id'}),
                   on='project_id',
                   how='left')
        )

        if df.empty:
             return SunburstChartData(labels=[], parents=[], values=[], metric_name=metric, max_level_processed=MAX_SUNBURST_LEVEL)

        # Explode the slash-delimited path
        # Handle potential None in 'sci_voc_path' by filling with empty string before split
        levels = df['sci_voc_path'].fillna('').str.strip('/').str.split('/', expand=True)
        levels = levels.rename(columns=lambda i: f'path_lvl_{i}')
        
        # Ensure all potential levels up to MAX_SUNBURST_LEVEL exist, even if paths are shorter
        for i in range(MAX_SUNBURST_LEVEL + 1):
            if f'path_lvl_{i}' not in levels.columns:
                levels[f'path_lvl_{i}'] = None # Add missing level columns as None

        df = pd.concat([df, levels], axis=1)

        # 3. Prepare data for sunburst
        labels = []
        parents = []
        values_dict = {m: [] for m in METRICS_LIST_SUNBURST}
        
        for lvl in range(MAX_SUNBURST_LEVEL + 1):
            lvl_col = f'path_lvl_{lvl}'
            parent_col = f'path_lvl_{lvl - 1}' if lvl > 0 else None

            # Filter out rows where the current level path is None or empty
            # This also handles projects not linked to any sci_voc or with short paths
            current_level_nodes_df = df[df[lvl_col].notna() & (df[lvl_col] != '')]
            if current_level_nodes_df.empty:
                continue # Skip if no nodes at this level

            uniques = current_level_nodes_df[lvl_col].unique().tolist()
            labels.extend(uniques)

            if lvl == 0:
                parents.extend([''] * len(uniques))
            else:
                # For each unique node, look up its parent
                # Ensure parent_col exists and is valid
                node_parents = []
                for node in uniques:
                    parent_series = current_level_nodes_df.loc[current_level_nodes_df[lvl_col] == node, parent_col]
                    if not parent_series.empty:
                        node_parents.append(parent_series.iloc[0])
                    else: # Should not happen if data is consistent
                        node_parents.append('') 
                parents.extend(node_parents)
            
            # Sum metrics for nodes at this level
            grp = current_level_nodes_df.groupby(lvl_col)
            for m_key in METRICS_LIST_SUNBURST:
                # Sum, reindex to match 'uniques' order, fill NaN with 0
                metric_sum = grp[m_key].sum(numeric_only=True).reindex(uniques).fillna(0).tolist()
                values_dict[m_key].extend(metric_sum)
        
        if not labels: # If no hierarchical data was processed
            return SunburstChartData(labels=[], parents=[], values=[], metric_name=metric, max_level_processed=MAX_SUNBURST_LEVEL)

        # Select values for the chosen metric
        final_values = values_dict.get(metric, [])
        # Ensure all values are float or None for Pydantic model
        final_values_cleaned = [float(v) if pd.notna(v) else None for v in final_values]


        return SunburstChartData(
            labels=labels,
            parents=parents,
            values=final_values_cleaned,
            metric_name=metric,
            max_level_processed=MAX_SUNBURST_LEVEL
        )

    except Exception as e:
        print(f"Error generating sunburst data: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred generating sunburst data: {str(e)}")




# --- FastAPI Endpoints (Add the new one) ---

@app.get("/")
async def read_root():
    return {"message": "Hello from FastAPI! CORS is enabled."}

# Placeholder for existing endpoints like /projects, /project/{project_id}, /projects/analytics,
# /projects/analytics/sunburst, /analytics/ec-by-country, /analytics/projects-by-country,
# /analytics/top-institutions-by-funding if you are adding to the same main.py

# Add these Pydantic models with your other models
class CountryFundingItem(BaseModel):
    country: str  # Country name or code, as it appears in your 'organizations' table
    funding: Optional[float]

class FundingByCountryResponse(BaseModel):
    chart_title: str = "Total EC Contribution by Country"
    color_axis_label: str = "EC Contribution (EUR)"
    data: List[CountryFundingItem]
    notes: Optional[str] = "Country codes/names are used as available in the organizations table. Funding represents the sum of EC contributions."

# Add this new endpoint function with your other FastAPI endpoints
@app.get("/projects/analytics/funding-by-country", response_model=FundingByCountryResponse, tags=["Analytics"])
async def get_funding_by_country_data():
    """
    Fetches data for a choropleth map of total EC funding by country.
    It sums EC contributions from the project_organizations table,
    grouping by country obtained from the organizations table.
    This endpoint does not convert country codes to ISO-3; it uses them as is.
    """
    try:
        # 1. Fetch data from Supabase
        # Fetch project_organizations (only relevant columns)
        # The table definition confirms 'organization_id' and 'ec_contribution' exist.
        po_response = supabase.table("project_organizations").select("organization_id, ec_contribution").execute()
        po_data = po_response.data
        if not po_data:
            return FundingByCountryResponse(data=[], notes="No project organization data found.")

        # Fetch organizations (only relevant columns: 'id' as PK, and 'country')
        # Assuming 'organizations' table has 'id' and 'country' columns.
        org_response = supabase.table("organizations").select("id, country").execute()
        org_data = org_response.data
        if not org_data:
            return FundingByCountryResponse(data=[], notes="No organization data found.")

        # 2. Convert to Pandas DataFrames
        po_df = pd.DataFrame(po_data)
        org_df = pd.DataFrame(org_data)

        if po_df.empty or org_df.empty:
             return FundingByCountryResponse(data=[], notes="Initial data for organizations or project participations is empty.")

        # 3. Data Cleaning and Preparation
        # Ensure 'ec_contribution' is numeric, coercing errors and filling NaN with 0
        po_df['ec_contribution'] = pd.to_numeric(po_df['ec_contribution'], errors='coerce').fillna(0)
        
        # Ensure join keys 'organization_id' (from po_df) and 'id' (from org_df) are of compatible types.
        # Supabase usually returns numeric types correctly. If issues arise, explicit casting like
        # po_df['organization_id'] = po_df['organization_id'].astype(int)
        # org_df['id'] = org_df['id'].astype(int)
        # or converting both to str for merging can be done. For now, we assume direct compatibility.

        # Filter out organizations with no country specified or empty country strings from org_df before merging
        org_df = org_df.dropna(subset=['country'])
        org_df = org_df[org_df['country'].astype(str).str.strip() != '']
        
        if org_df.empty:
            return FundingByCountryResponse(data=[], notes="No organizations with valid country information found.")


        # 4. Merge DataFrames
        # Merge project organization data with organization data to get country information
        merged_df = pd.merge(
            po_df,
            org_df,
            left_on='organization_id',  # Foreign key in project_organizations
            right_on='id',              # Primary key in organizations
            how='left'                  # Use left merge to keep all project participations
        )

        # After merge, 'country' column from org_df is now in merged_df.
        # If an organization_id from po_df doesn't exist in org_df, 'country' will be NaN for those rows.
        # Filter out rows where 'country' is NaN (i.e., no matching organization or organization had no country)
        merged_df = merged_df.dropna(subset=['country'])
        
        if merged_df.empty:
            return FundingByCountryResponse(data=[], notes="No project participations could be mapped to a valid country.")
            
        # 5. Sum up ec_contribution by country
        funding_by_country_sum = (
            merged_df
            .groupby('country', as_index=False)['ec_contribution']
            .sum()
            .rename(columns={'ec_contribution': 'funding'})
        )
        
        # Optional: Filter out countries with zero or negligible funding
        # funding_by_country_sum = funding_by_country_sum[funding_by_country_sum['funding'] > 0]

        # 6. Format for response
        response_data_items = [
            CountryFundingItem(country=row['country'], funding=row['funding'])
            for _, row in funding_by_country_sum.iterrows()
        ]
        
        if not response_data_items:
            return FundingByCountryResponse(data=[], notes="No funding data found after aggregation by country. This might occur if all mapped participations had zero EC contribution or other filtering criteria.")

        return FundingByCountryResponse(data=response_data_items)

    except Exception as e:
        print(f"Error generating funding by country data: {e}") # Log the error
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        raise HTTPException(status_code=500, detail=f"An error occurred generating funding by country data: {str(e)}")

@app.get("/analytics/project-timeline", response_model=ProjectTimelineResponse, tags=["Analytics"])
async def get_project_timeline(
    metric: str = Query('project_count', enum=['project_count', 'total_funding', 'average_duration'], description="Metric to track over time")
):
    """
    Returns time series data showing project trends over years (start dates).
    Suitable for line charts showing project evolution over time.
    """
    try:
        response = supabase.table("projects").select("start_date, total_cost, ec_max_contribution, duration_months").execute()
        projects_data = response.data
        if not projects_data:
            return ProjectTimelineResponse(chart_title="Project Timeline", y_axis_label="Count", data=[])

        df = pd.DataFrame(projects_data)
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df = df.dropna(subset=['start_date'])
        df['year'] = df['start_date'].dt.year

        if metric == 'project_count':
            timeline_data = df.groupby('year').size().reset_index(name='count')
            y_label = "Number of Projects"
            timeline_data['metric_value'] = timeline_data['count']
        elif metric == 'total_funding':
            df['ec_max_contribution'] = pd.to_numeric(df['ec_max_contribution'], errors='coerce').fillna(0)
            timeline_data = df.groupby('year')['ec_max_contribution'].sum().reset_index()
            y_label = "Total EC Contribution (EUR)"
            timeline_data['metric_value'] = timeline_data['ec_max_contribution']
        else:  # average_duration
            df['duration_months'] = pd.to_numeric(df['duration_months'], errors='coerce')
            timeline_data = df.groupby('year')['duration_months'].mean().reset_index()
            y_label = "Average Duration (Months)"
            timeline_data['metric_value'] = timeline_data['duration_months']

        result_data = [
            TimeSeriesData(year=int(row['year']), metric_value=float(row['metric_value']), metric_name=metric)
            for _, row in timeline_data.iterrows()
        ]

        return ProjectTimelineResponse(
            chart_title=f"Project {metric.replace('_', ' ').title()} Timeline",
            y_axis_label=y_label,
            data=result_data
        )
    except Exception as e:
        print(f"Error generating project timeline: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating project timeline: {str(e)}")

@app.get("/analytics/collaboration-network", response_model=NetworkGraphResponse, tags=["Analytics"])
async def get_collaboration_network(
    min_collaborations: int = Query(2, ge=1, description="Minimum number of collaborations to include in network")
):
    """
    Returns network data showing collaboration patterns between countries.
    Suitable for network graphs showing which countries collaborate most frequently.
    """
    try:
        po_response = supabase.table("project_organizations").select("project_id, organization_id").execute()
        org_response = supabase.table("organizations").select("id, country").execute()
        
        if not po_response.data or not org_response.data:
            return NetworkGraphResponse(chart_title="Country Collaboration Network", nodes=[], edges=[], description="No data available")

        po_df = pd.DataFrame(po_response.data)
        org_df = pd.DataFrame(org_response.data)
        
        merged_df = pd.merge(po_df, org_df, left_on='organization_id', right_on='id', how='left')
        merged_df = merged_df.dropna(subset=['country'])
        
        # Get country pairs per project
        project_countries = merged_df.groupby('project_id')['country'].apply(list).reset_index()
        
        # Count collaborations between country pairs
        collaboration_counts = {}
        country_project_counts = merged_df.groupby('country')['project_id'].nunique().to_dict()
        
        for _, row in project_countries.iterrows():
            countries = list(set(row['country']))  # Remove duplicates
            if len(countries) > 1:
                for i in range(len(countries)):
                    for j in range(i + 1, len(countries)):
                        pair = tuple(sorted([countries[i], countries[j]]))
                        collaboration_counts[pair] = collaboration_counts.get(pair, 0) + 1

        # Filter by minimum collaborations
        filtered_collaborations = {k: v for k, v in collaboration_counts.items() if v >= min_collaborations}
        
        # Create nodes and edges
        involved_countries = set()
        for pair in filtered_collaborations.keys():
            involved_countries.update(pair)
        
        nodes = [
            NetworkNode(
                id=country,
                label=country,
                size=float(country_project_counts.get(country, 1)),
                group="country"
            )
            for country in involved_countries
        ]
        
        edges = [
            NetworkEdge(source=pair[0], target=pair[1], weight=float(count))
            for pair, count in filtered_collaborations.items()
        ]

        return NetworkGraphResponse(
            chart_title="Country Collaboration Network",
            nodes=nodes,
            edges=edges,
            description=f"Shows countries that collaborated on at least {min_collaborations} projects together"
        )
    except Exception as e:
        print(f"Error generating collaboration network: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating collaboration network: {str(e)}")

@app.get("/analytics/funding-distribution", response_model=DistributionResponse, tags=["Analytics"])
async def get_funding_distribution(
    bin_count: int = Query(20, ge=5, le=50, description="Number of bins for the distribution")
):
    """
    Returns funding distribution data suitable for histogram visualization.
    Shows how project funding amounts are distributed.
    """
    try:
        response = supabase.table("projects").select("ec_max_contribution").execute()
        projects_data = response.data
        if not projects_data:
            return DistributionResponse(chart_title="Funding Distribution", data=[], chart_type="histogram", x_axis_label="Funding Range", y_axis_label="Count")

        df = pd.DataFrame(projects_data)
        df['ec_max_contribution'] = pd.to_numeric(df['ec_max_contribution'], errors='coerce')
        df = df.dropna(subset=['ec_max_contribution'])
        df = df[df['ec_max_contribution'] > 0]

        # Create bins
        hist, bin_edges = pd.cut(df['ec_max_contribution'], bins=bin_count, retbins=True, include_lowest=True)
        bin_counts = hist.value_counts().sort_index()

        result_data = []
        for interval, count in bin_counts.items():
            label = f"€{interval.left:,.0f} - €{interval.right:,.0f}"
            avg_value = (interval.left + interval.right) / 2
            result_data.append(DistributionData(label=label, value=float(avg_value), count=int(count)))

        return DistributionResponse(
            chart_title="Project Funding Distribution",
            data=result_data,
            chart_type="histogram",
            x_axis_label="Funding Range (EUR)",
            y_axis_label="Number of Projects"
        )
    except Exception as e:
        print(f"Error generating funding distribution: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating funding distribution: {str(e)}")

@app.get("/analytics/program-duration-heatmap", response_model=HeatmapResponse, tags=["Analytics"])
async def get_program_duration_heatmap():
    """
    Returns heatmap data showing average project duration by framework program and funding scheme.
    Suitable for heatmap visualization.
    """
    try:
        response = supabase.table("projects").select("framework_programme, funding_scheme, duration_months").execute()
        projects_data = response.data
        if not projects_data:
            return HeatmapResponse(chart_title="Program Duration Heatmap", data=[], x_axis_label="Framework Program", y_axis_label="Funding Scheme", color_scale_label="Average Duration (Months)")

        df = pd.DataFrame(projects_data)
        df['duration_months'] = pd.to_numeric(df['duration_months'], errors='coerce')
        df = df.dropna(subset=['framework_programme', 'funding_scheme', 'duration_months'])

        heatmap_data = df.groupby(['framework_programme', 'funding_scheme'])['duration_months'].mean().reset_index()

        result_data = [
            HeatmapData(
                x_category=row['framework_programme'],
                y_category=row['funding_scheme'],
                value=float(row['duration_months'])
            )
            for _, row in heatmap_data.iterrows()
        ]

        return HeatmapResponse(
            chart_title="Average Project Duration by Program and Scheme",
            data=result_data,
            x_axis_label="Framework Programme",
            y_axis_label="Funding Scheme",
            color_scale_label="Average Duration (Months)"
        )
    except Exception as e:
        print(f"Error generating program duration heatmap: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating program duration heatmap: {str(e)}")

@app.get("/analytics/efficiency-bubble-chart", response_model=BubbleChartResponse, tags=["Analytics"])
async def get_efficiency_bubble_chart():
    """
    Returns bubble chart data showing project efficiency metrics.
    X-axis: Total Cost, Y-axis: Duration, Bubble size: EC Contribution, Color: Framework Program
    """
    try:
        response = supabase.table("projects").select("acronym, total_cost, duration_months, ec_max_contribution, framework_programme").execute()
        projects_data = response.data
        if not projects_data:
            return BubbleChartResponse(chart_title="Project Efficiency Analysis", data=[], x_axis_label="Total Cost", y_axis_label="Duration", size_label="EC Contribution")

        df = pd.DataFrame(projects_data)
        numeric_cols = ['total_cost', 'duration_months', 'ec_max_contribution']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.dropna(subset=numeric_cols)
        df = df[(df['total_cost'] > 0) & (df['duration_months'] > 0) & (df['ec_max_contribution'] > 0)]

        result_data = [
            BubbleChartData(
                x=float(row['total_cost']),
                y=float(row['duration_months']),
                size=float(row['ec_max_contribution']),
                label=row['acronym'] or f"Project {row.name}",
                category=row['framework_programme'] or "Unknown"
            )
            for _, row in df.iterrows()
        ]

        return BubbleChartResponse(
            chart_title="Project Efficiency Analysis",
            data=result_data,
            x_axis_label="Total Cost (EUR)",
            y_axis_label="Duration (Months)",
            size_label="EC Contribution (EUR)"
        )
    except Exception as e:
        print(f"Error generating efficiency bubble chart: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating efficiency bubble chart: {str(e)}")

@app.get("/analytics/participation-trends", response_model=ProjectTimelineResponse, tags=["Analytics"])
async def get_participation_trends(
    country: str = Query(..., description="Country code/name to analyze participation trends")
):
    """
    Returns time series data showing a specific country's participation trends over time.
    Suitable for line charts showing how a country's involvement has evolved.
    """
    try:
        po_response = supabase.table("project_organizations").select("project_id, organization_id").execute()
        org_response = supabase.table("organizations").select("id, country").execute()
        proj_response = supabase.table("projects").select("id, start_date").execute()
        
        if not all([po_response.data, org_response.data, proj_response.data]):
            return ProjectTimelineResponse(chart_title=f"{country} Participation Trends", y_axis_label="Project Count", data=[])

        po_df = pd.DataFrame(po_response.data)
        org_df = pd.DataFrame(org_response.data)
        proj_df = pd.DataFrame(proj_response.data)

        # Filter organizations by country
        country_orgs = org_df[org_df['country'] == country]['id'].tolist()
        if not country_orgs:
            return ProjectTimelineResponse(chart_title=f"{country} Participation Trends", y_axis_label="Project Count", data=[])

        # Get projects for this country
        country_projects = po_df[po_df['organization_id'].isin(country_orgs)]['project_id'].unique()
        
        # Merge with project dates
        country_proj_df = proj_df[proj_df['id'].isin(country_projects)].copy()
        country_proj_df['start_date'] = pd.to_datetime(country_proj_df['start_date'], errors='coerce')
        country_proj_df = country_proj_df.dropna(subset=['start_date'])
        country_proj_df['year'] = country_proj_df['start_date'].dt.year

        # Count projects per year
        timeline_data = country_proj_df.groupby('year').size().reset_index(name='count')

        result_data = [
            TimeSeriesData(year=int(row['year']), metric_value=float(row['count']), metric_name="project_count")
            for _, row in timeline_data.iterrows()
        ]

        return ProjectTimelineResponse(
            chart_title=f"{country} Project Participation Trends",
            y_axis_label="Number of Projects",
            data=result_data
        )
    except Exception as e:
        print(f"Error generating participation trends: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating participation trends: {str(e)}")

@app.get("/analytics/research-output-timeline", response_model=PublicationTimelineResponse, tags=["Analytics"])
async def get_research_output_timeline():
    """
    Analyzes research output over time using publications data.
    Shows publication trends and research productivity.
    """
    try:
        pub_response = supabase.table("publications").select("published_year, project_id").execute()
        if not pub_response.data:
            return PublicationTimelineResponse(data=[])

        pub_df = pd.DataFrame(pub_response.data)
        pub_df['published_year'] = pd.to_numeric(pub_df['published_year'], errors='coerce')
        pub_df = pub_df.dropna(subset=['published_year'])
        pub_df = pub_df[(pub_df['published_year'] >= 2000) & (pub_df['published_year'] <= 2024)]

        # Count publications per year
        pub_per_year = pub_df.groupby('published_year').size().reset_index(name='publication_count')
        
        # Count unique projects with publications per year
        projects_per_year = pub_df.groupby('published_year')['project_id'].nunique().reset_index(name='projects_with_publications')
        
        # Merge and calculate average
        timeline_df = pd.merge(pub_per_year, projects_per_year, on='published_year')
        timeline_df['avg_publications_per_project'] = timeline_df['publication_count'] / timeline_df['projects_with_publications']

        result_data = [
            PublicationAnalysis(
                year=int(row['published_year']),
                publication_count=int(row['publication_count']),
                projects_with_publications=int(row['projects_with_publications']),
                avg_publications_per_project=float(row['avg_publications_per_project'])
            )
            for _, row in timeline_df.iterrows()
        ]

        return PublicationTimelineResponse(data=result_data)
    except Exception as e:
        print(f"Error generating research output timeline: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating research output timeline: {str(e)}")

@app.get("/analytics/topic-evolution", response_model=TopicEvolutionResponse, tags=["Analytics"])
async def get_topic_evolution():
    """
    Shows how different research topics have evolved over time in terms of project count and funding.
    Creates a stream graph or area chart visualization.
    """
    try:
        # Get project-topic relationships and project details
        proj_response = supabase.table("projects").select("id, start_date, ec_max_contribution").execute()
        topic_response = supabase.table("topics").select("code, title").execute()
        
        # Assuming there's a project_topics junction table
        try:
            proj_topic_response = supabase.table("project_topics").select("project_id, topic_code").execute()
        except:
            # If project_topics doesn't exist, create mock data based on project fields
            proj_topic_response = None

        if not proj_response.data or not topic_response.data:
            return TopicEvolutionResponse(data=[])

        proj_df = pd.DataFrame(proj_response.data)
        topic_df = pd.DataFrame(topic_response.data)
        
        proj_df['start_date'] = pd.to_datetime(proj_df['start_date'], errors='coerce')
        proj_df = proj_df.dropna(subset=['start_date'])
        proj_df['year'] = proj_df['start_date'].dt.year
        proj_df['ec_max_contribution'] = pd.to_numeric(proj_df['ec_max_contribution'], errors='coerce').fillna(0)

        if proj_topic_response and proj_topic_response.data:
            proj_topic_df = pd.DataFrame(proj_topic_response.data)
            merged_df = pd.merge(proj_topic_df, proj_df, left_on='project_id', right_on='id')
            merged_df = pd.merge(merged_df, topic_df, left_on='topic_code', right_on='code')
        else:
            # Fallback: use project fields as topics
            field_projects = proj_df[proj_df['field'].notna()].copy()
            field_projects['topic'] = field_projects['field']
            merged_df = field_projects

        if merged_df.empty:
            return TopicEvolutionResponse(data=[])

        # Group by topic and year
        topic_evolution = merged_df.groupby(['title' if 'title' in merged_df.columns else 'topic', 'year']).agg({
            'id': 'count',
            'ec_max_contribution': 'sum'
        }).reset_index()

        topic_evolution.columns = ['topic', 'year', 'project_count', 'funding_amount']

        result_data = [
            TopicEvolutionData(
                topic=row['topic'],
                year=int(row['year']),
                project_count=int(row['project_count']),
                funding_amount=float(row['funding_amount'])
            )
            for _, row in topic_evolution.iterrows()
        ]

        return TopicEvolutionResponse(data=result_data)
    except Exception as e:
        print(f"Error generating topic evolution: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating topic evolution: {str(e)}")

@app.get("/analytics/project-success-metrics", response_model=ProjectSuccessResponse, tags=["Analytics"])
async def get_project_success_metrics():
    """
    Analyzes project success based on publications output, funding efficiency, and collaboration.
    Creates a sophisticated scatter plot with multiple dimensions.
    """
    try:
        proj_response = supabase.table("projects").select("id, acronym, duration_months, ec_max_contribution").execute()
        pub_response = supabase.table("publications").select("project_id").execute()
        po_response = supabase.table("project_organizations").select("project_id, organization_id").execute()
        org_response = supabase.table("organizations").select("id, country").execute()

        if not all([proj_response.data, pub_response.data, po_response.data, org_response.data]):
            return ProjectSuccessResponse(data=[])

        proj_df = pd.DataFrame(proj_response.data)
        pub_df = pd.DataFrame(pub_response.data)
        po_df = pd.DataFrame(po_response.data)
        org_df = pd.DataFrame(org_response.data)

        # Count publications per project
        pub_counts = pub_df.groupby('project_id').size().reset_index(name='publications_count')
        
        # Count countries per project (collaboration score)
        po_org_df = pd.merge(po_df, org_df, left_on='organization_id', right_on='id')
        country_counts = po_org_df.groupby('project_id')['country'].nunique().reset_index(name='collaboration_score')
        
        # Merge all data
        success_df = pd.merge(proj_df, pub_counts, left_on='id', right_on='project_id', how='left')
        success_df = pd.merge(success_df, country_counts, left_on='id', right_on='project_id', how='left')
        
        # Fill missing values
        success_df['publications_count'] = success_df['publications_count'].fillna(0)
        success_df['collaboration_score'] = success_df['collaboration_score'].fillna(1)
        
        # Calculate funding efficiency (publications per million EUR)
        success_df['ec_max_contribution'] = pd.to_numeric(success_df['ec_max_contribution'], errors='coerce').fillna(1)
        success_df['duration_months'] = pd.to_numeric(success_df['duration_months'], errors='coerce').fillna(12)
        success_df['funding_efficiency'] = success_df['publications_count'] / (success_df['ec_max_contribution'] / 1000000)
        success_df['funding_efficiency'] = success_df['funding_efficiency'].replace([float('inf')], 0)
        
        # Filter for projects with some activity
        success_df = success_df[(success_df['publications_count'] > 0) | (success_df['ec_max_contribution'] > 100000)]
        success_df = success_df.head(100)  # Limit for visualization

        result_data = [
            ProjectSuccessMetrics(
                project_acronym=row['acronym'] or f"Project_{row['id']}",
                duration_months=int(row['duration_months']),
                publications_count=int(row['publications_count']),
                funding_efficiency=float(row['funding_efficiency']),
                collaboration_score=int(row['collaboration_score'])
            )
            for _, row in success_df.iterrows()
        ]

        return ProjectSuccessResponse(data=result_data)
    except Exception as e:
        print(f"Error generating project success metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating project success metrics: {str(e)}")

@app.get("/analytics/research-field-network", response_model=ResearchFieldNetworkResponse, tags=["Analytics"])
async def get_research_field_network():
    """
    Creates a network showing relationships between research fields based on shared projects and publications.
    """
    try:
        proj_response = supabase.table("projects").select("id, field, sub_field, ec_max_contribution").execute()
        pub_response = supabase.table("publications").select("project_id").execute()
        
        if not proj_response.data:
            return ResearchFieldNetworkResponse(nodes=[], edges=[])

        proj_df = pd.DataFrame(proj_response.data)
        pub_df = pd.DataFrame(pub_response.data) if pub_response.data else pd.DataFrame()
        
        # Count publications per project
        if not pub_df.empty:
            pub_counts = pub_df.groupby('project_id').size().reset_index(name='publications_count')
            proj_df = pd.merge(proj_df, pub_counts, left_on='id', right_on='project_id', how='left')
        proj_df['publications_count'] = proj_df.get('publications_count', 0).fillna(0)
        
        # Clean and prepare field data
        proj_df['field'] = proj_df['field'].fillna('Unknown')
        proj_df['ec_max_contribution'] = pd.to_numeric(proj_df['ec_max_contribution'], errors='coerce').fillna(0)
        
        # Aggregate by field
        field_stats = proj_df.groupby('field').agg({
            'id': 'count',
            'publications_count': 'sum',
            'ec_max_contribution': 'sum'
        }).reset_index()
        field_stats.columns = ['field', 'project_count', 'total_publications', 'total_funding']
        
        # Create nodes
        nodes = []
        for _, row in field_stats.iterrows():
            if row['project_count'] > 1:  # Only include fields with multiple projects
                nodes.append(ResearchFieldNetworkNode(
                    id=row['field'],
                    label=row['field'],
                    size=float(row['project_count']),
                    publications=int(row['total_publications']),
                    funding=float(row['total_funding'])
                ))
        
        # Create edges based on shared sub-fields or common patterns
        edges = []
        if 'sub_field' in proj_df.columns:
            # Find fields that share sub-fields
            field_subfield = proj_df.groupby(['field', 'sub_field']).size().reset_index(name='count')
            subfield_fields = field_subfield.groupby('sub_field')['field'].apply(list).reset_index()
            
            for _, row in subfield_fields.iterrows():
                fields = row['field']
                if len(fields) > 1:
                    for i in range(len(fields)):
                        for j in range(i + 1, len(fields)):
                            edge_weight = 1.0  # Could be weighted by shared project count
                            edges.append(NetworkEdge(
                                source=fields[i],
                                target=fields[j],
                                weight=edge_weight
                            ))

        return ResearchFieldNetworkResponse(nodes=nodes, edges=edges)
    except Exception as e:
        print(f"Error generating research field network: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating research field network: {str(e)}")

@app.get("/analytics/project-seasonality", response_model=SeasonalityResponse, tags=["Analytics"])
async def get_project_seasonality():
    """
    Analyzes seasonal patterns in project starts and funding allocation.
    Creates a polar/radar chart showing monthly patterns.
    """
    try:
        response = supabase.table("projects").select("start_date, ec_max_contribution").execute()
        if not response.data:
            return SeasonalityResponse(data=[])

        df = pd.DataFrame(response.data)
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df = df.dropna(subset=['start_date'])
        df['month'] = df['start_date'].dt.month
        df['ec_max_contribution'] = pd.to_numeric(df['ec_max_contribution'], errors='coerce').fillna(0)

        # Group by month
        monthly_stats = df.groupby('month').agg({
            'start_date': 'count',
            'ec_max_contribution': 'mean'
        }).reset_index()
        monthly_stats.columns = ['month', 'project_starts', 'avg_funding']

        # Add month names
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        monthly_stats['month_name'] = monthly_stats['month'].apply(lambda x: month_names[x-1])

        result_data = [
            SeasonalityData(
                month=int(row['month']),
                month_name=row['month_name'],
                project_starts=int(row['project_starts']),
                avg_funding=float(row['avg_funding'])
            )
            for _, row in monthly_stats.iterrows()
        ]

        return SeasonalityResponse(data=result_data)
    except Exception as e:
        print(f"Error generating project seasonality: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating project seasonality: {str(e)}")