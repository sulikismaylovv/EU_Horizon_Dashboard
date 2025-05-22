# backend/init_env.py

import sys
from pathlib import Path

def init_project(module_root: str="backend") -> None:
    """
    Add the project root (one level above `module_root`) to sys.path.
    """
    project_root = Path(__file__).resolve().parent
    while project_root.name != module_root and project_root.parent != project_root:
        project_root = project_root.parent

    root = project_root.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
