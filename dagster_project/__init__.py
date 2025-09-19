from dagster import Definitions

# Import the definitions
from .definitions import defs

# This makes the definitions available when the package is imported
__all__ = ["defs"]