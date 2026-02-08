"""
Módulos del proyecto ANI Scraping refactorizado para Airflow.
"""

from .extraction import run_extraction
from .validation import run_validation
from .persistence import run_persistence

__all__ = ['run_extraction', 'run_validation', 'run_persistence']
