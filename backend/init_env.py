# backend/init_env.py

import sys
from pathlib import Path

def set_project_root():
    """
    Adds the project root to sys.path so you can import anything from anywhere.
    """
    project_root = Path(__file__).resolve().parent.parent
    sys.path.append(str(project_root))
