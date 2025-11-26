#!/usr/bin/env python3
"""
DAG Workflow Example
====================

This example demonstrates creating and executing DAG (Directed Acyclic Graph)
workflows with the Job Orchestrator. It covers:

- Building DAGs with DAGBuilder
- Defining job dependencies
- Parallel and sequential execution
- DAG validation
- Execution planning
- Progress tracking

Run this example:
    python examples/dag_workflow.py
"""

import time
import random
from typing import Any

from job_orchestrator import (
    Job,
    JobPriority,
    DAG,
    DAGBuilder,
)
from job_orchestrator.scheduler import Scheduler


# =============================================================================
# Job Functions for DAG Examples
# =============================================================================

def extract_from_source_a() -> dict[str, Any]:
    """Extract data from source A."""
    print("  [Extract A] Fetching data from source A...")
    time.sleep(0.3)
    data = {"source": "A", "records": [1, 2, 3, 4, 5]}
    print(f"  [Extract A] Got {len(data['records'])} records")
    return data


def extract_from_source_b() -> dict[str, Any]:
    """Extract data from source B."""
    print("  [Extract B] Fetching data from source B...")
    time.sleep(0.4)
    data = {"source": "B", "records": [10, 20, 30]}
    print(f"  [Extract B] Got {len(data['records'])} records")
    return data


def extract_from_source_c() -> dict[str, Any]:
    """Extract data from source C."""
    print("  [Extract C] Fetching data from source C...")
    time.sleep(0.2)
    data = {"source": "C", "records": [100, 200]}
    print(f"  [Extract C] Got {len(data['records'])} records")
    return data


def transform_data() -> dict[str, Any]:
    """Transform extracted data."""
    print("  [Transform] Processing data...")
    time.sleep(0.5)
    result = {"transformed": True, "record_count": 10}
    print("  [Transform] Transformation complete")
    return result


def validate_data() -> bool:
    """Validate transformed data."""
    print("  [Validate] Validating data quality...")
    time.sleep(0.2)
    print("  [Validate] Data is valid")
    return True


def load_to_warehouse() -> dict[str, Any]:
    """Load data to data warehouse."""
    print("  [Load] Loading to warehouse...")
    time.sleep(0.4)
    result = {"loaded": True, "rows": 10}
    print("  [Load] Load complete")
    return result


def generate_report() -> str:
    """Generate final report."""
    print("  [Report] Generating report...")
    time.sleep(0.3)
    report = "Pipeline completed successfully"
    print(f"  [Report] {report}")
    return report


def send_notification() -> bool:
    """Send completion notification."""
    print("  [Notify] Sending notification...")
    time.sleep(0.1)
    print("  [Notify] Notification sent")
    return True


def cleanup() -> None:
    """Cleanup temporary resources."""
    print("  [Cleanup] Cleaning up...")
    time.sleep(0.1)
    print("  [Cleanup] Done")


# =============================================================================
# Example Functions
# =============================================================================

def example_simple_dag() -> None:
    """Example 1: Simple linear DAG."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Linear DAG (A → B → C)")
    print("=" * 60)
    
    # Build a simple linear DAG
    dag = (DAGBuilder("simple_pipeline")
        .add_job(extract_from_source_a, job_id="extract")
        .add_job(transform_data, job_id="transform", depends_on=["extract"])
        .add_job(load_to_warehouse, job_id="load", depends_on=["transform"])
        .build())
    
    print(f"DAG: {dag.name}")
    print(f"Jobs: {len(dag.nodes)}")
    
    # Get execution plan
    execution_plan = dag.get_execution_plan()
    print("\nExecution plan (jobs per level):")
    for level, jobs in enumerate(execution_plan):
        job_names = [j.name for j in jobs]
        print(f"  Level {level}: {job_names}")
    
    # Validate DAG
    is_valid = dag.validate()
    print(f"\nDAG is valid: {is_valid}")
    
    # Execute
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        print("\nExecuting DAG...")
        for job in dag.topological_sort():
            result = scheduler.run_job(job)
            print(f"  {job.name}: {result.state.name}")
    finally:
        scheduler.stop()


def example_parallel_dag() -> None:
    """Example 2: DAG with parallel branches."""
    print("\n" + "=" * 60)
    print("Example 2: Parallel DAG (fan-out/fan-in)")
    print("=" * 60)
    
    # Build DAG with parallel extraction
    #
    #        extract_a ─┐
    #                   │
    # start ─ extract_b ─┼─ transform ─ load
    #                   │
    #        extract_c ─┘
    #
    dag = (DAGBuilder("parallel_pipeline")
        .add_job(extract_from_source_a, job_id="extract_a")
        .add_job(extract_from_source_b, job_id="extract_b")
        .add_job(extract_from_source_c, job_id="extract_c")
        .add_job(transform_data, job_id="transform",
                 depends_on=["extract_a", "extract_b", "extract_c"])
        .add_job(load_to_warehouse, job_id="load", depends_on=["transform"])
        .build())
    
    print(f"DAG: {dag.name}")
    
    # Show execution plan
    execution_plan = dag.get_execution_plan()
    print("\nExecution plan:")
    for level, jobs in enumerate(execution_plan):
        job_names = [j.name for j in jobs]
        parallel = "parallel" if len(jobs) > 1 else "sequential"
        print(f"  Level {level} ({parallel}): {job_names}")
    
    # Execute level by level
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        print("\nExecuting DAG level by level...")
        for level, jobs in enumerate(execution_plan):
            print(f"\n  Level {level}:")
            # In a real scenario, these would run in parallel
            for job in jobs:
                result = scheduler.run_job(job)
                print(f"    {job.name}: {result.state.name}")
    finally:
        scheduler.stop()


def example_complex_dag() -> None:
    """Example 3: Complex DAG with multiple paths."""
    print("\n" + "=" * 60)
    print("Example 3: Complex DAG with Multiple Paths")
    print("=" * 60)
    
    # Build a complex DAG:
    #
    #  extract_a ─┬─ transform ─┬─ load ─ notify
    #             │             │
    #  extract_b ─┤             ├─ report
    #             │             │
    #  extract_c ─┴─ validate ──┘
    #
    dag = (DAGBuilder("complex_pipeline", "Complex ETL with validation")
        # Extract layer
        .add_job(extract_from_source_a, job_id="extract_a")
        .add_job(extract_from_source_b, job_id="extract_b")
        .add_job(extract_from_source_c, job_id="extract_c")
        
        # Transform layer
        .add_job(transform_data, job_id="transform",
                 depends_on=["extract_a", "extract_b", "extract_c"])
        .add_job(validate_data, job_id="validate",
                 depends_on=["extract_a", "extract_b", "extract_c"])
        
        # Load layer
        .add_job(load_to_warehouse, job_id="load",
                 depends_on=["transform", "validate"])
        .add_job(generate_report, job_id="report",
                 depends_on=["transform", "validate"])
        
        # Notification layer
        .add_job(send_notification, job_id="notify",
                 depends_on=["load"])
        
        .with_fail_fast(True)
        .with_max_parallel(3)
        .build())
    
    print(f"DAG: {dag.name}")
    print(f"Description: {dag.description}")
    print(f"Total jobs: {len(dag.nodes)}")
    print(f"Fail fast: {dag.fail_fast}")
    print(f"Max parallel: {dag.max_parallel}")
    
    # Analyze DAG structure
    root_nodes = dag.get_root_nodes()
    leaf_nodes = dag.get_leaf_nodes()
    print(f"\nRoot nodes (no dependencies): {root_nodes}")
    print(f"Leaf nodes (no dependents): {leaf_nodes}")
    
    # Execute
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        print("\nExecuting complex DAG...")
        execution_plan = dag.get_execution_plan()
        
        total_jobs = len(dag.nodes)
        completed = 0
        
        for level, jobs in enumerate(execution_plan):
            print(f"\n  Level {level}: {[j.name for j in jobs]}")
            for job in jobs:
                result = scheduler.run_job(job)
                completed += 1
                progress = (completed / total_jobs) * 100
                print(f"    {job.name}: {result.state.name} ({progress:.0f}% complete)")
                
    finally:
        scheduler.stop()


def example_dag_with_priorities() -> None:
    """Example 4: DAG with job priorities."""
    print("\n" + "=" * 60)
    print("Example 4: DAG with Job Priorities")
    print("=" * 60)
    
    # Create jobs with explicit priorities
    extract_job = Job(
        name="extract",
        func=extract_from_source_a,
        priority=JobPriority.NORMAL,
    )
    
    transform_job = Job(
        name="transform",
        func=transform_data,
        priority=JobPriority.HIGH,  # Higher priority
    )
    
    load_job = Job(
        name="load",
        func=load_to_warehouse,
        priority=JobPriority.CRITICAL,  # Highest priority
    )
    
    # Build DAG with Job objects
    dag = DAG(name="priority_pipeline")
    dag.add_node(extract_job)
    dag.add_node(transform_job)
    dag.add_node(load_job)
    dag.add_edge(extract_job.id, transform_job.id)
    dag.add_edge(transform_job.id, load_job.id)
    
    dag.validate()
    
    print("Jobs by priority:")
    for job in dag.topological_sort():
        print(f"  {job.name}: {job.priority.name}")
    
    # Execute
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        print("\nExecuting DAG...")
        for job in dag.topological_sort():
            result = scheduler.run_job(job)
            print(f"  {job.name}: {result.state.name}")
    finally:
        scheduler.stop()


def example_dag_validation() -> None:
    """Example 5: DAG validation and cycle detection."""
    print("\n" + "=" * 60)
    print("Example 5: DAG Validation")
    print("=" * 60)
    
    # Valid DAG
    print("Creating valid DAG...")
    valid_dag = (DAGBuilder("valid_dag")
        .add_job(extract_from_source_a, job_id="a")
        .add_job(transform_data, job_id="b", depends_on=["a"])
        .add_job(load_to_warehouse, job_id="c", depends_on=["b"])
        .build())
    
    print(f"  Has cycle: {valid_dag.has_cycle()}")
    print(f"  Is valid: {valid_dag.validate()}")
    
    # Try to create a DAG with cycle (will fail validation)
    print("\nCreating DAG with potential cycle...")
    cyclic_dag = DAG(name="cyclic_dag")
    
    job_a = Job(name="job_a", func=extract_from_source_a)
    job_b = Job(name="job_b", func=transform_data)
    job_c = Job(name="job_c", func=load_to_warehouse)
    
    cyclic_dag.add_node(job_a)
    cyclic_dag.add_node(job_b)
    cyclic_dag.add_node(job_c)
    
    # Create edges: a -> b -> c -> a (cycle!)
    cyclic_dag.add_edge(job_a.id, job_b.id)
    cyclic_dag.add_edge(job_b.id, job_c.id)
    cyclic_dag.add_edge(job_c.id, job_a.id)  # This creates a cycle
    
    print(f"  Has cycle: {cyclic_dag.has_cycle()}")
    try:
        cyclic_dag.validate()
    except Exception as e:
        print(f"  Validation failed: {type(e).__name__}")


def example_dag_execution_tracking() -> None:
    """Example 6: Tracking DAG execution."""
    print("\n" + "=" * 60)
    print("Example 6: DAG Execution Tracking")
    print("=" * 60)
    
    dag = (DAGBuilder("tracked_pipeline")
        .add_job(extract_from_source_a, job_id="extract")
        .add_job(transform_data, job_id="transform", depends_on=["extract"])
        .add_job(load_to_warehouse, job_id="load", depends_on=["transform"])
        .build())
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        print("Tracking DAG execution:\n")
        
        # Execute and track
        for i, job in enumerate(dag.topological_sort()):
            print(f"Step {i + 1}/{len(dag.nodes)}: {job.name}")
            print(f"  State before: {job.state.name}")
            
            result = scheduler.run_job(job)
            
            print(f"  State after: {result.state.name}")
            print(f"  Execution time: {result.execution_time:.3f}s")
            
            if result.result:
                print(f"  Result: {result.result}")
            print()
            
        print(f"DAG complete: {dag.is_complete}")
        print(f"DAG progress: {dag.progress * 100:.0f}%")
        
    finally:
        scheduler.stop()


def example_dag_with_failure() -> None:
    """Example 7: DAG with failure handling."""
    print("\n" + "=" * 60)
    print("Example 7: DAG with Failure (Fail-Fast)")
    print("=" * 60)
    
    def failing_transform():
        print("  [Transform] Starting...")
        time.sleep(0.2)
        raise ValueError("Transform failed!")
    
    dag = (DAGBuilder("failing_pipeline")
        .add_job(extract_from_source_a, job_id="extract")
        .add_job(failing_transform, job_id="transform", depends_on=["extract"])
        .add_job(load_to_warehouse, job_id="load", depends_on=["transform"])
        .with_fail_fast(True)
        .build())
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        print("Executing DAG with fail-fast enabled...\n")
        
        for job in dag.topological_sort():
            print(f"Running: {job.name}")
            result = scheduler.run_job(job)
            print(f"  State: {result.state.name}")
            
            if result.state.name == "FAILED":
                print(f"  Error: {result.error}")
                if dag.fail_fast:
                    print("\nFail-fast triggered. Stopping DAG execution.")
                    break
                    
        print(f"\nDAG has failed: {dag.has_failed}")
        print(f"DAG progress: {dag.progress * 100:.0f}%")
        
    finally:
        scheduler.stop()


def main() -> None:
    """Run all DAG examples."""
    print("=" * 60)
    print("Job Orchestrator - DAG Workflow Examples")
    print("=" * 60)
    
    example_simple_dag()
    example_parallel_dag()
    example_complex_dag()
    example_dag_with_priorities()
    example_dag_validation()
    example_dag_execution_tracking()
    example_dag_with_failure()
    
    print("\n" + "=" * 60)
    print("All DAG examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()