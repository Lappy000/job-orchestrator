"""
Storage module for the Job Orchestrator.

Provides persistence backends for job state and results.
Supports in-memory, Redis, and PostgreSQL backends.
"""

from .base import BaseStorage, InMemoryStorage

__all__ = [
    "BaseStorage",
    "InMemoryStorage",
]