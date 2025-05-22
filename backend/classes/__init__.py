"""
classes package

This module provides access to core data classes used throughout the EU Horizon Dashboard project:
- CORDIS_data: Loads and enriches the full CORDIS dataset.
- Project_data: Extracts detailed information about a specific project based on ID or acronym.
"""

from .cordis_data import CORDIS_data
from .project_data import Project_data

__all__ = [
    "CORDIS_data",
    "Project_data"
]
