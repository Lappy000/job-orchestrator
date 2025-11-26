#!/usr/bin/env python3
"""
ETL Pipeline Example
====================

This example demonstrates a complete ETL (Extract, Transform, Load) pipeline
with the Job Orchestrator. It covers:

- Real-world ETL pattern implementation
- Error handling and recovery
- Data validation
- Checkpoint/restart capability
- Progress reporting
- Dead Letter Queue handling

Run this example:
    python examples/etl_pipeline.py
"""

import time
import random
from datetime import datetime
from typing import Any
from dataclasses import dataclass, field

from job_orchestrator import (
    Job,
    JobState,
    JobPriority,
    RetryPolicy,
    DAGBuilder,
    OrchestratorConfig,
)
from job_orchestrator.scheduler import Scheduler


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class DataRecord:
    """Represents a data record in the pipeline."""
    id: int
    source: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    is_valid: bool = True
    error_message: str = ""


@dataclass
class PipelineContext:
    """Context shared across pipeline stages."""
    pipeline_id: str
    start_time: datetime
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    checkpoints: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# Global context for the pipeline (in production, use proper state management)
pipeline_context: PipelineContext | None = None


# =============================================================================
# Extract Stage
# =============================================================================

def extract_from_database(source_name: str, count: int) -> list[dict[str, Any]]:
    """
    Extract records from a database source.
    
    Simulates database extraction with potential failures.
    """
    global pipeline_context
    print(f"  [Extract] Reading from {source_name}...")
    
    records = []
    for i in range(count):
        # Simulate occasional extraction failures
        if random.random() < 0.05:
            print(f"  [Extract] Warning: Failed to read record {i}")
            continue
            
        record = {
            "id": i + 1,
            "source": source_name,
            "value": random.uniform(0, 1000),
            "timestamp": datetime.utcnow().isoformat(),
        }
        records.append(record)
        
        # Simulate network latency
        time.sleep(0.01)
    
    if pipeline_context:
        pipeline_context.records_extracted += len(records)
        pipeline_context.checkpoints.append(f"extracted_{source_name}")
    
    print(f"  [Extract] Got {len(records)} records from {source_name}")
    return records


def extract_from_api(endpoint: str, page_size: int = 10) -> list[dict[str, Any]]:
    """
    Extract records from an API endpoint.
    
    Simulates paginated API calls.
    """
    global pipeline_context
    print(f"  [Extract] Fetching from API: {endpoint}...")
    
    records = []
    
    # Simulate pagination
    for page in range(3):
        print(f"  [Extract] Fetching page {page + 1}...")
        time.sleep(0.1)  # API latency
        
        for i in range(page_size):
            record = {
                "id": page * page_size + i + 1000,
                "source": "api",
                "value": random.uniform(0, 500),
                "timestamp": datetime.utcnow().isoformat(),
            }
            records.append(record)
    
    if pipeline_context:
        pipeline_context.records_extracted += len(records)
        pipeline_context.checkpoints.append("extracted_api")
    
    print(f"  [Extract] Got {len(records)} records from API")
    return records


def extract_from_file(file_path: str) -> list[dict[str, Any]]:
    """
    Extract records from a file.
    
    Simulates file reading.
    """
    global pipeline_context
    print(f"  [Extract] Reading file: {file_path}...")
    
    time.sleep(0.2)  # File I/O simulation
    
    records = [
        {"id": 2001, "source": "file", "value": 123.45, "timestamp": datetime.utcnow().isoformat()},
        {"id": 2002, "source": "file", "value": 678.90, "timestamp": datetime.utcnow().isoformat()},
        {"id": 2003, "source": "file", "value": 111.11, "timestamp": datetime.utcnow().isoformat()},
    ]
    
    if pipeline_context:
        pipeline_context.records_extracted += len(records)
        pipeline_context.checkpoints.append("extracted_file")
    
    print(f"  [Extract] Got {len(records)} records from file")
    return records


# =============================================================================
# Transform Stage
# =============================================================================

def validate_records(records: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    """
    Validate extracted records.
    
    Returns tuple of (valid_records, invalid_records).
    """
    global pipeline_context
    print(f"  [Validate] Checking {len(records)} records...")
    
    valid = []
    invalid = []
    
    for record in records:
        # Validation rules
        errors = []
        
        if record.get("value", 0) < 0:
            errors.append("Negative value")
        if not record.get("source"):
            errors.append("Missing source")
        if record.get("value", 0) > 900:  # Business rule
            errors.append("Value exceeds threshold")
        
        if errors:
            record["errors"] = errors
            invalid.append(record)
        else:
            valid.append(record)
    
    if pipeline_context:
        pipeline_context.records_failed += len(invalid)
        pipeline_context.checkpoints.append("validated")
    
    print(f"  [Validate] Valid: {len(valid)}, Invalid: {len(invalid)}")
    return valid, invalid


def transform_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Transform validated records.
    
    Applies business transformations.
    """
    global pipeline_context
    print(f"  [Transform] Processing {len(records)} records...")
    
    transformed = []
    
    for record in records:
        # Apply transformations
        transformed_record = {
            "id": record["id"],
            "source": record["source"].upper(),
            "original_value": record["value"],
            "value_normalized": record["value"] / 1000.0,
            "value_category": "high" if record["value"] > 500 else "low",
            "processed_at": datetime.utcnow().isoformat(),
            "pipeline_version": "1.0",
        }
        transformed.append(transformed_record)
        
        # Simulate processing time
        time.sleep(0.005)
    
    if pipeline_context:
        pipeline_context.records_transformed = len(transformed)
        pipeline_context.checkpoints.append("transformed")
    
    print(f"  [Transform] Transformed {len(transformed)} records")
    return transformed


def enrich_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Enrich records with additional data.
    
    Simulates external data lookup.
    """
    global pipeline_context
    print(f"  [Enrich] Enriching {len(records)} records...")
    
    enriched = []
    
    for record in records:
        # Simulate external lookup
        record["region"] = random.choice(["US-EAST", "US-WEST", "EU", "APAC"])
        record["priority"] = "critical" if record["value_category"] == "high" else "normal"
        enriched.append(record)
    
    if pipeline_context:
        pipeline_context.checkpoints.append("enriched")
    
    print(f"  [Enrich] Enriched {len(enriched)} records")
    return enriched


# =============================================================================
# Load Stage
# =============================================================================

def load_to_warehouse(records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Load transformed records to data warehouse.
    
    Simulates batch insert with potential failures.
    """
    global pipeline_context
    print(f"  [Load] Writing {len(records)} records to warehouse...")
    
    # Simulate batch loading
    batch_size = 10
    loaded = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        # Simulate occasional write failures
        if random.random() < 0.02:
            print(f"  [Load] Warning: Batch {i // batch_size} failed, retrying...")
            time.sleep(0.1)
        
        loaded += len(batch)
        time.sleep(0.05)  # Write latency
    
    if pipeline_context:
        pipeline_context.records_loaded = loaded
        pipeline_context.checkpoints.append("loaded_warehouse")
    
    result = {
        "destination": "warehouse",
        "records_loaded": loaded,
        "load_time": datetime.utcnow().isoformat(),
    }
    
    print(f"  [Load] Successfully loaded {loaded} records")
    return result


def write_to_error_log(records: list[dict[str, Any]]) -> int:
    """
    Write invalid records to error log.
    """
    global pipeline_context
    print(f"  [Error Log] Writing {len(records)} failed records...")
    
    time.sleep(0.1)
    
    if pipeline_context:
        pipeline_context.checkpoints.append("errors_logged")
    
    print(f"  [Error Log] Logged {len(records)} errors")
    return len(records)


def update_metrics(stats: dict[str, Any]) -> None:
    """
    Update pipeline metrics.
    """
    print(f"  [Metrics] Updating pipeline metrics...")
    print(f"  [Metrics] Extracted: {stats.get('extracted', 0)}")
    print(f"  [Metrics] Transformed: {stats.get('transformed', 0)}")
    print(f"  [Metrics] Loaded: {stats.get('loaded', 0)}")
    print(f"  [Metrics] Failed: {stats.get('failed', 0)}")


def send_completion_alert(success: bool, message: str) -> bool:
    """
    Send pipeline completion notification.
    """
    status = "SUCCESS" if success else "FAILURE"
    print(f"  [Alert] Pipeline {status}: {message}")
    return True


# =============================================================================
# Pipeline Execution
# =============================================================================

def run_simple_etl_pipeline() -> None:
    """Example 1: Simple sequential ETL pipeline."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Sequential ETL Pipeline")
    print("=" * 60)
    
    global pipeline_context
    pipeline_context = PipelineContext(
        pipeline_id=f"pipeline_{int(time.time())}",
        start_time=datetime.utcnow(),
    )
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        # Build simple pipeline
        dag = (DAGBuilder("simple_etl", "Basic ETL pipeline")
            .add_job(
                lambda: extract_from_database("customers", 20),
                job_id="extract"
            )
            .add_job(
                lambda: validate_records(pipeline_context.checkpoints),  # Placeholder
                job_id="validate",
                depends_on=["extract"]
            )
            .add_job(transform_records, job_id="transform", depends_on=["validate"])
            .add_job(load_to_warehouse, job_id="load", depends_on=["transform"])
            .with_fail_fast(True)
            .build())
        
        # Execute manually with data passing
        print(f"\nStarting pipeline: {pipeline_context.pipeline_id}")
        
        # Extract
        print("\n--- Extract Phase ---")
        records = extract_from_database("customers", 20)
        
        # Validate
        print("\n--- Validate Phase ---")
        valid_records, invalid_records = validate_records(records)
        
        if invalid_records:
            write_to_error_log(invalid_records)
        
        # Transform
        print("\n--- Transform Phase ---")
        transformed = transform_records(valid_records)
        
        # Load
        print("\n--- Load Phase ---")
        result = load_to_warehouse(transformed)
        
        # Report
        print("\n--- Pipeline Summary ---")
        print(f"Pipeline ID: {pipeline_context.pipeline_id}")
        print(f"Records extracted: {pipeline_context.records_extracted}")
        print(f"Records transformed: {pipeline_context.records_transformed}")
        print(f"Records loaded: {pipeline_context.records_loaded}")
        print(f"Records failed: {pipeline_context.records_failed}")
        print(f"Checkpoints: {pipeline_context.checkpoints}")
        
        send_completion_alert(True, f"Loaded {result['records_loaded']} records")
        
    finally:
        scheduler.stop()


def run_parallel_extract_pipeline() -> None:
    """Example 2: ETL with parallel extraction."""
    print("\n" + "=" * 60)
    print("Example 2: Parallel Extract ETL Pipeline")
    print("=" * 60)
    
    global pipeline_context
    pipeline_context = PipelineContext(
        pipeline_id=f"parallel_{int(time.time())}",
        start_time=datetime.utcnow(),
    )
    
    scheduler = Scheduler()
    scheduler.start()
    
    try:
        print(f"\nStarting pipeline: {pipeline_context.pipeline_id}")
        
        # Parallel extraction (simulated)
        print("\n--- Parallel Extract Phase ---")
        all_records = []
        
        # In production, these would run concurrently
        db_records = extract_from_database("orders", 15)
        all_records.extend(db_records)
        
        api_records = extract_from_api("/api/transactions", page_size=5)
        all_records.extend(api_records)
        
        file_records = extract_from_file("/data/archive.csv")
        all_records.extend(file_records)
        
        print(f"\n  Total records extracted: {len(all_records)}")
        
        # Validate all records
        print("\n--- Validate Phase ---")
        valid, invalid = validate_records(all_records)
        
        # Transform
        print("\n--- Transform Phase ---")
        transformed = transform_records(valid)
        
        # Enrich
        print("\n--- Enrich Phase ---")
        enriched = enrich_records(transformed)
        
        # Load
        print("\n--- Load Phase ---")
        result = load_to_warehouse(enriched)
        
        # Update metrics
        print("\n--- Metrics Phase ---")
        update_metrics({
            "extracted": len(all_records),
            "transformed": len(transformed),
            "loaded": result["records_loaded"],
            "failed": len(invalid),
        })
        
        # Final summary
        print("\n--- Pipeline Summary ---")
        print(f"Total sources: 3 (database, api, file)")
        print(f"Records extracted: {len(all_records)}")
        print(f"Records valid: {len(valid)}")
        print(f"Records enriched: {len(enriched)}")
        print(f"Records loaded: {result['records_loaded']}")
        
        send_completion_alert(True, "Multi-source pipeline complete")
        
    finally:
        scheduler.stop()


def run_robust_etl_pipeline() -> None:
    """Example 3: Robust ETL with error handling and retries."""
    print("\n" + "=" * 60)
    print("Example 3: Robust ETL Pipeline with Error Handling")
    print("=" * 60)
    
    global pipeline_context
    pipeline_context = PipelineContext(
        pipeline_id=f"robust_{int(time.time())}",
        start_time=datetime.utcnow(),
    )
    
    # Configure with retry policies
    config = OrchestratorConfig.from_dict({
        "retry": {
            "max_retries": 3,
            "base_delay": 1.0,
            "exponential_base": 2.0,
        },
        "dlq": {
            "max_size": 1000,
            "ttl_days": 7,
        },
    })
    
    scheduler = Scheduler(config)
    scheduler.start()
    
    # Track job results
    job_results: dict[str, Any] = {}
    
    # Callback handlers
    def on_complete(job: Job, result: Any) -> None:
        job_results[job.name] = {"status": "completed", "result": result}
        print(f"  ✓ {job.name} completed")
    
    def on_failed(job: Job, result: Any) -> None:
        job_results[job.name] = {"status": "failed", "error": str(result)}
        print(f"  ✗ {job.name} failed")
        
    scheduler.on_job_complete(on_complete)
    scheduler.on_job_failed(on_failed)
    
    try:
        print(f"\nStarting robust pipeline: {pipeline_context.pipeline_id}")
        
        # Create jobs with retry policies
        extract_retry = RetryPolicy(max_retries=3, base_delay=0.5)
        load_retry = RetryPolicy(max_retries=5, base_delay=1.0)
        
        # Execute with error handling
        print("\n--- Extract Phase (with retries) ---")
        extract_job = Job(
            name="extract_orders",
            func=lambda: extract_from_database("orders", 25),
            retry_policy=extract_retry,
            priority=JobPriority.HIGH,
        )
        
        extract_result = scheduler.run_job(extract_job)
        if extract_result.state != JobState.COMPLETED:
            print("  Extract failed, checking DLQ...")
            raise Exception("Extract phase failed")
        
        records = extract_result.result
        
        # Validate with error collection
        print("\n--- Validate Phase ---")
        valid, invalid = validate_records(records)
        
        if invalid:
            print(f"  {len(invalid)} records failed validation")
            # In production, these would go to DLQ or error table
            
        # Transform
        print("\n--- Transform Phase ---")
        transform_job = Job(
            name="transform_orders",
            func=lambda: transform_records(valid),
        )
        transform_result = scheduler.run_job(transform_job)
        transformed = transform_result.result
        
        # Load with retries
        print("\n--- Load Phase (with retries) ---")
        load_job = Job(
            name="load_orders",
            func=lambda: load_to_warehouse(transformed),
            retry_policy=load_retry,
            priority=JobPriority.CRITICAL,
            timeout=30.0,
        )
        
        load_result = scheduler.run_job(load_job)
        
        if load_result.state == JobState.COMPLETED:
            print("\n--- Pipeline Complete ---")
            update_metrics({
                "extracted": len(records),
                "transformed": len(transformed),
                "loaded": load_result.result["records_loaded"],
                "failed": len(invalid),
            })
            send_completion_alert(True, "Robust pipeline succeeded")
        else:
            print("\n--- Pipeline Failed ---")
            send_completion_alert(False, f"Load failed: {load_result.error}")
            
        # Show DLQ status
        dlq_stats = scheduler.get_dlq_stats()
        print(f"\nDLQ entries: {dlq_stats.total_entries}")
        
    except Exception as e:
        print(f"\nPipeline error: {e}")
        send_completion_alert(False, str(e))
        
    finally:
        scheduler.stop()


def run_incremental_etl_pipeline() -> None:
    """Example 4: Incremental ETL with checkpointing."""
    print("\n" + "=" * 60)
    print("Example 4: Incremental ETL with Checkpointing")
    print("=" * 60)
    
    global pipeline_context
    pipeline_context = PipelineContext(
        pipeline_id=f"incremental_{int(time.time())}",
        start_time=datetime.utcnow(),
    )
    
    scheduler = Scheduler()
    scheduler.start()
    
    # Simulate checkpoint storage
    checkpoints: dict[str, Any] = {
        "last_extracted_id": 0,
        "last_run": None,
    }
    
    try:
        print(f"\nStarting incremental pipeline: {pipeline_context.pipeline_id}")
        print(f"Last checkpoint: record_id={checkpoints['last_extracted_id']}")
        
        # Extract only new records
        print("\n--- Incremental Extract Phase ---")
        start_id = checkpoints["last_extracted_id"]
        records = []
        
        for i in range(start_id + 1, start_id + 11):
            records.append({
                "id": i,
                "source": "incremental",
                "value": random.uniform(0, 500),
                "timestamp": datetime.utcnow().isoformat(),
            })
        
        print(f"  Extracted {len(records)} new records (IDs {start_id + 1}-{start_id + 10})")
        pipeline_context.records_extracted = len(records)
        
        # Validate
        print("\n--- Validate Phase ---")
        valid, invalid = validate_records(records)
        
        # Transform
        print("\n--- Transform Phase ---")
        transformed = transform_records(valid)
        
        # Load
        print("\n--- Load Phase ---")
        result = load_to_warehouse(transformed)
        
        # Update checkpoint
        if records:
            checkpoints["last_extracted_id"] = max(r["id"] for r in records)
            checkpoints["last_run"] = datetime.utcnow().isoformat()
        
        print("\n--- Checkpoint Update ---")
        print(f"  New checkpoint: record_id={checkpoints['last_extracted_id']}")
        print(f"  Last run: {checkpoints['last_run']}")
        
        # Summary
        print("\n--- Pipeline Summary ---")
        print(f"Records processed: {len(records)}")
        print(f"Records loaded: {result['records_loaded']}")
        print("Ready for next incremental run")
        
    finally:
        scheduler.stop()


def main() -> None:
    """Run all ETL pipeline examples."""
    print("=" * 60)
    print("Job Orchestrator - ETL Pipeline Examples")
    print("=" * 60)
    
    run_simple_etl_pipeline()
    run_parallel_extract_pipeline()
    run_robust_etl_pipeline()
    run_incremental_etl_pipeline()
    
    print("\n" + "=" * 60)
    print("All ETL pipeline examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()