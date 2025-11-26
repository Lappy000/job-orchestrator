"""
Job Orchestrator Examples
=========================

This package contains runnable examples demonstrating various features
of the Job Orchestrator library.

Examples:
---------

- **simple_job.py**: Basic job submission and execution
- **dag_workflow.py**: DAG workflows with dependencies
- **etl_pipeline.py**: Complete ETL pipeline with error handling
- **distributed_locking.py**: Using distributed locks
- **worker_pool_scaling.py**: Dynamic worker pool with auto-scaling

Running Examples:
-----------------

Each example can be run directly:

    python -m examples.simple_job
    python -m examples.dag_workflow
    python -m examples.etl_pipeline
    python -m examples.distributed_locking
    python -m examples.worker_pool_scaling

Or from the examples directory:

    cd examples
    python simple_job.py
"""

__all__ = [
    "simple_job",
    "dag_workflow",
    "etl_pipeline",
    "distributed_locking",
    "worker_pool_scaling",
]