#!/usr/bin/env python3
"""
Simple Job Example
==================

This example demonstrates basic job submission and execution with the
Job Orchestrator. It covers:

- Creating a scheduler
- Defining job functions
- Submitting jobs with different priorities
- Running jobs synchronously
- Checking job status
- Handling job results

Run this example:
    python examples/simple_job.py
"""

import time
from typing import Any

from job_orchestrator import (
    Job,
    JobState,
    JobPriority,
    RetryPolicy,
    OrchestratorConfig,
)
from job_orchestrator.scheduler import Scheduler


# =============================================================================
# Job Functions
# =============================================================================

def add_numbers(a: int, b: int) -> int:
    """Simple job that adds two numbers."""
    print(f"  Adding {a} + {b}")
    return a + b


def multiply_numbers(a: int, b: int) -> int:
    """Simple job that multiplies two numbers."""
    print(f"  Multiplying {a} * {b}")
    return a * b


def process_data(data: list[Any]) -> dict[str, Any]:
    """Process a list of data items."""
    print(f"  Processing {len(data)} items")
    time.sleep(0.5)  # Simulate work
    return {
        "count": len(data),
        "sum": sum(data) if all(isinstance(x, (int, float)) for x in data) else None,
        "processed": True,
    }


def slow_task(duration: float) -> str:
    """A slow task that takes some time."""
    print(f"  Starting slow task ({duration}s)")
    time.sleep(duration)
    print(f"  Slow task completed")
    return f"Completed after {duration}s"


def failing_task() -> None:
    """A task that always fails (for demonstration)."""
    print("  This task will fail...")
    raise ValueError("Intentional failure for demonstration")


# =============================================================================
# Example Functions
# =============================================================================

def example_basic_job() -> None:
    """Example 1: Basic job submission and execution."""
    print("\n" + "=" * 60)
    print("Example 1: Basic Job Submission")
    print("=" * 60)
    
    # Create scheduler with default configuration
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create a simple job
        job = Job(
            name="add_numbers",
            func=add_numbers,
            args=(10, 20),
        )
        
        # Submit the job
        job_id = scheduler.submit(job)
        print(f"Submitted job with ID: {job_id}")
        
        # Run the job synchronously
        result = scheduler.run_job(job)
        
        print(f"Job state: {result.state}")
        print(f"Job result: {result.result}")
        print(f"Execution time: {result.execution_time:.3f}s")
        
    finally:
        scheduler.stop()


def example_job_priorities() -> None:
    """Example 2: Jobs with different priorities."""
    print("\n" + "=" * 60)
    print("Example 2: Job Priorities")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create jobs with different priorities
        jobs = [
            Job(name="background_task", func=add_numbers, args=(1, 1),
                priority=JobPriority.BACKGROUND),
            Job(name="low_priority", func=add_numbers, args=(2, 2),
                priority=JobPriority.LOW),
            Job(name="normal_task", func=add_numbers, args=(3, 3),
                priority=JobPriority.NORMAL),
            Job(name="high_priority", func=add_numbers, args=(4, 4),
                priority=JobPriority.HIGH),
            Job(name="critical_task", func=add_numbers, args=(5, 5),
                priority=JobPriority.CRITICAL),
        ]
        
        # Submit all jobs
        for job in jobs:
            scheduler.submit(job)
            print(f"Submitted: {job.name} (priority: {job.priority.name})")
        
        # Process jobs - they will be processed in priority order
        print("\nProcessing order (highest priority first):")
        processed = 0
        while processed < len(jobs):
            job = scheduler.get_next_job(timeout=1.0)
            if job:
                result = scheduler.run_job(job)
                print(f"  {processed + 1}. {job.name}: {result.result}")
                processed += 1
                
    finally:
        scheduler.stop()


def example_job_with_kwargs() -> None:
    """Example 3: Jobs with keyword arguments."""
    print("\n" + "=" * 60)
    print("Example 3: Jobs with Keyword Arguments")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create job with kwargs
        job = Job(
            name="process_data",
            func=process_data,
            kwargs={"data": [1, 2, 3, 4, 5]},
        )
        
        result = scheduler.run_job(job)
        print(f"Result: {result.result}")
        
    finally:
        scheduler.stop()


def example_retry_policy() -> None:
    """Example 4: Jobs with retry policy."""
    print("\n" + "=" * 60)
    print("Example 4: Retry Policy")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create retry policy
        retry_policy = RetryPolicy(
            max_retries=3,
            base_delay=0.5,
            max_delay=5.0,
            exponential_base=2.0,
            jitter=False,  # Disable jitter for predictable delays
        )
        
        print("Retry policy configuration:")
        print(f"  Max retries: {retry_policy.max_retries}")
        print(f"  Base delay: {retry_policy.base_delay}s")
        print(f"  Exponential base: {retry_policy.exponential_base}")
        
        # Calculate expected delays
        for attempt in range(retry_policy.max_retries):
            delay = retry_policy.calculate_delay(attempt)
            print(f"  Delay for attempt {attempt + 1}: {delay:.2f}s")
        
        # Create job with retry policy
        job = Job(
            name="failing_task",
            func=failing_task,
            retry_policy=retry_policy,
        )
        
        print("\nRunning job (will fail and exhaust retries)...")
        result = scheduler.run_job(job)
        
        print(f"\nFinal state: {result.state}")
        print(f"Error: {result.error}")
        print(f"Retry count: {job.retry_count}")
        
    finally:
        scheduler.stop()


def example_job_timeout() -> None:
    """Example 5: Jobs with timeout."""
    print("\n" + "=" * 60)
    print("Example 5: Job Timeout")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create job with timeout shorter than execution time
        job = Job(
            name="slow_task",
            func=slow_task,
            args=(5.0,),  # Task takes 5 seconds
            timeout=1.0,  # But timeout is 1 second
        )
        
        print(f"Job timeout: {job.timeout}s")
        print("Running job (will timeout)...")
        
        result = scheduler.run_job(job)
        
        print(f"Final state: {result.state}")
        if result.error:
            print(f"Error: {result.error}")
            
    finally:
        scheduler.stop()


def example_job_metadata() -> None:
    """Example 6: Jobs with metadata and tags."""
    print("\n" + "=" * 60)
    print("Example 6: Job Metadata and Tags")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create job with metadata and tags
        job = Job(
            name="process_order",
            func=process_data,
            kwargs={"data": [100, 200, 300]},
            tags={
                "type": "order_processing",
                "customer_id": "CUST-123",
                "region": "US-WEST",
            },
            metadata={
                "source": "web_api",
                "version": "1.0",
                "request_id": "req-abc-123",
            },
        )
        
        print("Job tags:")
        for key, value in job.tags.items():
            print(f"  {key}: {value}")
        
        print("\nJob metadata:")
        for key, value in job.metadata.items():
            print(f"  {key}: {value}")
        
        result = scheduler.run_job(job)
        print(f"\nResult: {result.result}")
        
    finally:
        scheduler.stop()


def example_job_status_tracking() -> None:
    """Example 7: Tracking job status."""
    print("\n" + "=" * 60)
    print("Example 7: Job Status Tracking")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Create and submit a job
        job = Job(
            name="tracked_job",
            func=slow_task,
            args=(1.0,),
        )
        
        job_id = scheduler.submit(job)
        print(f"Job ID: {job_id}")
        
        # Check initial status
        status = scheduler.get_job_status(str(job.id))
        print(f"Initial status: {status}")
        
        # Get full job info
        retrieved_job = scheduler.get_job(str(job.id))
        if retrieved_job:
            print(f"Job name: {retrieved_job.name}")
            print(f"Is active: {retrieved_job.is_active}")
            print(f"Is terminal: {retrieved_job.is_terminal}")
        
        # Run the job
        result = scheduler.run_job(job)
        
        # Check final status
        print(f"Final state: {result.state}")
        print(f"Execution time: {result.execution_time:.3f}s")
        
    finally:
        scheduler.stop()


def example_callbacks() -> None:
    """Example 8: Using callbacks for job events."""
    print("\n" + "=" * 60)
    print("Example 8: Job Callbacks")
    print("=" * 60)
    
    completed_jobs: list[str] = []
    failed_jobs: list[str] = []
    
    def on_job_complete(job: Job, result: Any) -> None:
        completed_jobs.append(job.name)
        print(f"  [Callback] Job completed: {job.name}")
    
    def on_job_failed(job: Job, result: Any) -> None:
        failed_jobs.append(job.name)
        print(f"  [Callback] Job failed: {job.name}")
    
    scheduler = Scheduler()
    scheduler.on_job_complete(on_job_complete)
    scheduler.on_job_failed(on_job_failed)
    scheduler.start()
    
    try:
        # Submit successful job
        job1 = Job(name="successful_job", func=add_numbers, args=(1, 2))
        scheduler.run_job(job1)
        
        # Submit failing job (with no retries)
        job2 = Job(
            name="failing_job",
            func=failing_task,
            retry_policy=RetryPolicy(max_retries=0),
        )
        scheduler.run_job(job2)
        
        print(f"\nCompleted jobs: {completed_jobs}")
        print(f"Failed jobs: {failed_jobs}")
        
    finally:
        scheduler.stop()


def main() -> None:
    """Run all examples."""
    print("=" * 60)
    print("Job Orchestrator - Simple Job Examples")
    print("=" * 60)
    
    example_basic_job()
    example_job_priorities()
    example_job_with_kwargs()
    example_retry_policy()
    example_job_timeout()
    example_job_metadata()
    example_job_status_tracking()
    example_callbacks()
    
    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()