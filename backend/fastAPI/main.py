
from fastapi import FastAPI, HTTPException, Query
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

# --- Pydantic Models ---
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


# --- FastAPI Endpoints ---
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

