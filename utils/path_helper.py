import os
import sys

def add_project_to_path():
    """Add the project root to Python path to enable imports."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
