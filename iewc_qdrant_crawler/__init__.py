# __init__.py
# This file marks the directory as a Python package.
# The function is registered in function_app.py at the root level.

# Export the run function for easier imports
from .crawler import run

__all__ = ['run']