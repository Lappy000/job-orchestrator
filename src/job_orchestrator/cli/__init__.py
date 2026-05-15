"""
CLI interface for the Job Orchestrator.

Provides command-line tools for managing jobs, DAGs, and scheduler
operations without writing Python code.
"""

from .main import cli

__all__ = ["cli"]
