#!/usr/bin/env python3
"""
Worker Pool Scaling Example
===========================

This example demonstrates dynamic worker pool management with the Job
Orchestrator. It covers:

- Creating and configuring worker pools
- Manual scaling (scale up/down)
- Auto-scaling based on utilization
- Worker health monitoring
- Pool statistics and metrics
- Different worker types (thread, process)

Run this example:
    python examples/worker_pool_scaling.py
"""

import time
import random
import threading
from datetime import datetime
from typing import Any
from concurrent.futures import ThreadPoolExecutor

from job_orchestrator import Job, JobPriority, OrchestratorConfig
from job_orchestrator.scheduler import Scheduler
from job_orchestrator.workers import WorkerPool, PoolConfig, WorkerType


# =============================================================================
# Job Functions
# =============================================================================

def cpu_intensive_task(iterations: int = 1000000) -> dict[str, Any]:
    """CPU-bound task for testing CPU workers."""
    start = time.time()
    
    # Simulate CPU work
    result = 0
    for i in range(iterations):
        result += i * i % 1000
    
    duration = time.time() - start
    return {
        "type": "cpu_intensive",
        "iterations": iterations,
        "result": result,
        "duration": duration,
    }


def io_bound_task(wait_time: float = 0.5) -> dict[str, Any]:
    """I/O-bound task for testing thread workers."""
    start = time.time()
    
    # Simulate I/O wait (network, disk, etc.)
    time.sleep(wait_time)
    
    duration = time.time() - start
    return {
        "type": "io_bound",
        "wait_time": wait_time,
        "duration": duration,
    }


def variable_duration_task() -> dict[str, Any]:
    """Task with variable duration for testing auto-scaling."""
    duration = random.uniform(0.1, 1.0)
    time.sleep(duration)
    return {
        "type": "variable",
        "duration": duration,
    }


def quick_task() -> str:
    """Quick task for high-throughput testing."""
    time.sleep(0.01)
    return "done"


def slow_task() -> str:
    """Slow task for testing pool saturation."""
    time.sleep(2.0)
    return "done"


# =============================================================================
# Example Functions
# =============================================================================

def example_basic_worker_pool() -> None:
    """Example 1: Basic worker pool setup."""
    print("\n" + "=" * 60)
    print("Example 1: Basic Worker Pool Setup")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    # Create basic pool configuration
    config = PoolConfig(
        min_workers=2,
        max_workers=8,
        worker_type=WorkerType.THREAD,
    )
    
    print(f"\nPool Configuration:")
    print(f"  Min workers: {config.min_workers}")
    print(f"  Max workers: {config.max_workers}")
    print(f"  Worker type: {config.worker_type.name}")
    print(f"  Scale up threshold: {config.scale_up_threshold}")
    print(f"  Scale down threshold: {config.scale_down_threshold}")
    
    # Create and start pool
    pool = WorkerPool(scheduler, config)
    pool.start()
    
    print(f"\nPool started with {pool.worker_count} workers")
    
    # Get initial stats
    stats = pool.get_stats()
    print(f"\nInitial Pool Stats:")
    print(f"  Total workers: {stats.total_workers}")
    print(f"  Busy workers: {stats.busy_workers}")
    print(f"  Idle workers: {stats.idle_workers}")
    print(f"  Utilization: {stats.utilization:.1%}")
    
    # Stop pool
    pool.stop(wait=True)
    scheduler.stop()
    print("\nPool stopped")


def example_manual_scaling() -> None:
    """Example 2: Manual scaling operations."""
    print("\n" + "=" * 60)
    print("Example 2: Manual Scaling")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    config = PoolConfig(
        min_workers=2,
        max_workers=10,
        worker_type=WorkerType.THREAD,
    )
    
    pool = WorkerPool(scheduler, config)
    pool.start()
    
    print(f"\nInitial workers: {pool.worker_count}")
    
    # Scale up
    print("\nScaling up by 3 workers...")
    added = pool.scale_up(3)
    print(f"  Workers added: {added}")
    print(f"  Current workers: {pool.worker_count}")
    
    # Scale up more
    print("\nScaling up by 5 more workers...")
    added = pool.scale_up(5)
    print(f"  Workers added: {added}")
    print(f"  Current workers: {pool.worker_count}")
    
    # Try to exceed max
    print("\nTrying to scale beyond max...")
    added = pool.scale_up(10)
    print(f"  Workers added: {added} (limited by max_workers)")
    print(f"  Current workers: {pool.worker_count}")
    
    # Scale down
    print("\nScaling down by 5 workers...")
    removed = pool.scale_down(5)
    print(f"  Workers removed: {removed}")
    print(f"  Current workers: {pool.worker_count}")
    
    # Try to go below min
    print("\nTrying to scale below min...")
    removed = pool.scale_down(10)
    print(f"  Workers removed: {removed} (limited by min_workers)")
    print(f"  Current workers: {pool.worker_count}")
    
    pool.stop(wait=True)
    scheduler.stop()


def example_pool_under_load() -> None:
    """Example 3: Worker pool under load."""
    print("\n" + "=" * 60)
    print("Example 3: Pool Under Load")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    config = PoolConfig(
        min_workers=4,
        max_workers=8,
        worker_type=WorkerType.THREAD,
    )
    
    pool = WorkerPool(scheduler, config)
    pool.start()
    
    print(f"\nStarting with {pool.worker_count} workers")
    
    # Submit many jobs
    num_jobs = 20
    print(f"\nSubmitting {num_jobs} I/O-bound jobs...")
    
    jobs = []
    for i in range(num_jobs):
        job = Job(
            name=f"io_job_{i}",
            func=io_bound_task,
            args=(0.3,),  # 300ms each
        )
        scheduler.submit(job)
        jobs.append(job)
    
    # Monitor pool stats while processing
    print("\nMonitoring pool (press Ctrl+C to stop early):")
    
    start_time = time.time()
    completed = 0
    
    try:
        while completed < num_jobs and time.time() - start_time < 30:
            stats = pool.get_stats()
            
            # Count completed jobs
            completed = sum(1 for j in jobs if j.state.name in ("COMPLETED", "FAILED"))
            
            print(f"  [{time.time() - start_time:.1f}s] "
                  f"Workers: {stats.busy_workers}/{stats.total_workers} busy, "
                  f"Utilization: {stats.utilization:.0%}, "
                  f"Completed: {completed}/{num_jobs}")
            
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nInterrupted")
    
    # Final stats
    print(f"\nFinal Stats:")
    stats = pool.get_stats()
    print(f"  Jobs completed: {stats.jobs_completed}")
    print(f"  Jobs failed: {stats.jobs_failed}")
    print(f"  Avg job time: {stats.avg_job_time:.3f}s")
    
    pool.stop(wait=True)
    scheduler.stop()


def example_auto_scaling() -> None:
    """Example 4: Auto-scaling based on utilization."""
    print("\n" + "=" * 60)
    print("Example 4: Auto-Scaling")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    # Configure auto-scaling
    config = PoolConfig(
        min_workers=2,
        max_workers=10,
        worker_type=WorkerType.THREAD,
        scale_up_threshold=0.7,    # Scale up when 70% utilized
        scale_down_threshold=0.3,  # Scale down when 30% utilized
        scale_interval=2.0,        # Check every 2 seconds
        worker_max_idle_time=5.0,  # Remove idle workers after 5 seconds
    )
    
    print(f"\nAuto-scaling Configuration:")
    print(f"  Scale up threshold: {config.scale_up_threshold:.0%}")
    print(f"  Scale down threshold: {config.scale_down_threshold:.0%}")
    print(f"  Scale interval: {config.scale_interval}s")
    print(f"  Worker max idle time: {config.worker_max_idle_time}s")
    
    pool = WorkerPool(scheduler, config)
    pool.start()
    
    print(f"\nStarting with {pool.worker_count} workers")
    
    # Phase 1: Submit burst of jobs (should trigger scale up)
    print("\n--- Phase 1: High Load (should scale up) ---")
    for i in range(15):
        job = Job(name=f"burst_{i}", func=io_bound_task, args=(0.5,))
        scheduler.submit(job)
    
    # Monitor scaling
    for _ in range(10):
        stats = pool.get_stats()
        print(f"  Workers: {stats.total_workers}, "
              f"Utilization: {stats.utilization:.0%}")
        time.sleep(1.0)
    
    # Phase 2: Let jobs complete (should trigger scale down)
    print("\n--- Phase 2: Low Load (should scale down) ---")
    for _ in range(15):
        stats = pool.get_stats()
        print(f"  Workers: {stats.total_workers}, "
              f"Utilization: {stats.utilization:.0%}, "
              f"Idle: {stats.idle_workers}")
        time.sleep(1.0)
    
    print(f"\nFinal worker count: {pool.worker_count}")
    
    pool.stop(wait=True)
    scheduler.stop()


def example_worker_types() -> None:
    """Example 5: Different worker types."""
    print("\n" + "=" * 60)
    print("Example 5: Worker Types")
    print("=" * 60)
    
    print("""
Worker Types and their use cases:

1. THREAD (ThreadPoolExecutor)
   - Best for: I/O-bound tasks (network, disk)
   - Pros: Low overhead, shared memory
   - Cons: GIL limits CPU parallelism
   
2. PROCESS (ProcessPoolExecutor)
   - Best for: CPU-bound tasks
   - Pros: True parallelism, GIL bypass
   - Cons: Higher overhead, IPC serialization
   
3. ASYNC (AsyncIO)
   - Best for: High-concurrency I/O
   - Pros: Very efficient for async code
   - Cons: Only works with async functions
""")
    
    # Demonstrate thread workers
    print("\nThread Workers for I/O-bound tasks:")
    scheduler = Scheduler()
    scheduler.start()
    
    thread_config = PoolConfig(
        min_workers=4,
        max_workers=20,  # Can have many threads for I/O
        worker_type=WorkerType.THREAD,
    )
    pool = WorkerPool(scheduler, thread_config)
    pool.start()
    
    # Submit I/O jobs
    for i in range(10):
        job = Job(name=f"io_{i}", func=io_bound_task, args=(0.2,))
        scheduler.submit(job)
    
    time.sleep(3)
    
    stats = pool.get_stats()
    print(f"  Thread pool - Completed: {stats.jobs_completed}, "
          f"Avg time: {stats.avg_job_time:.3f}s")
    
    pool.stop(wait=True)
    scheduler.stop()
    
    # Note about process workers
    print("\nProcess Workers for CPU-bound tasks:")
    print("  (Process workers would be configured with WorkerType.PROCESS)")
    print("  Best used when tasks involve heavy computation")


def example_health_monitoring() -> None:
    """Example 6: Worker health monitoring."""
    print("\n" + "=" * 60)
    print("Example 6: Worker Health Monitoring")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    config = PoolConfig(
        min_workers=3,
        max_workers=6,
        worker_type=WorkerType.THREAD,
        health_check_interval=2.0,
        worker_heartbeat_timeout=10.0,
    )
    
    pool = WorkerPool(scheduler, config)
    pool.start()
    
    print(f"\nHealth monitoring configuration:")
    print(f"  Health check interval: {config.health_check_interval}s")
    print(f"  Heartbeat timeout: {config.worker_heartbeat_timeout}s")
    
    # Get worker info
    print(f"\nWorker Information:")
    worker_info = pool.get_worker_info()
    
    for info in worker_info:
        print(f"  Worker {info.worker_id}:")
        print(f"    State: {info.state}")
        print(f"    Jobs completed: {info.jobs_completed}")
        print(f"    Current job: {info.current_job or 'None'}")
    
    # Submit some jobs
    print("\nSubmitting jobs...")
    for i in range(6):
        job = Job(name=f"health_test_{i}", func=io_bound_task, args=(0.5,))
        scheduler.submit(job)
    
    # Monitor worker states
    print("\nMonitoring workers during execution:")
    for _ in range(5):
        time.sleep(1.0)
        
        stats = pool.get_stats()
        worker_info = pool.get_worker_info()
        
        healthy = sum(1 for w in worker_info if w.state != "unhealthy")
        
        print(f"  Healthy workers: {healthy}/{len(worker_info)}, "
              f"Busy: {stats.busy_workers}, "
              f"Jobs done: {stats.jobs_completed}")
    
    pool.stop(wait=True)
    scheduler.stop()


def example_pool_statistics() -> None:
    """Example 7: Detailed pool statistics."""
    print("\n" + "=" * 60)
    print("Example 7: Pool Statistics")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    config = PoolConfig(
        min_workers=4,
        max_workers=8,
        worker_type=WorkerType.THREAD,
    )
    
    pool = WorkerPool(scheduler, config)
    pool.start()
    
    # Process a variety of jobs
    print("\nProcessing jobs...")
    for i in range(20):
        duration = random.uniform(0.1, 0.5)
        job = Job(
            name=f"stats_job_{i}",
            func=io_bound_task,
            args=(duration,),
        )
        scheduler.submit(job)
    
    # Wait for completion
    time.sleep(5)
    
    # Get detailed statistics
    stats = pool.get_stats()
    
    print("\n" + "-" * 40)
    print("Pool Statistics")
    print("-" * 40)
    print(f"  Total workers: {stats.total_workers}")
    print(f"  Busy workers: {stats.busy_workers}")
    print(f"  Idle workers: {stats.idle_workers}")
    print(f"  Utilization: {stats.utilization:.1%}")
    print()
    print(f"  Jobs completed: {stats.jobs_completed}")
    print(f"  Jobs failed: {stats.jobs_failed}")
    print(f"  Jobs in queue: {stats.jobs_in_queue}")
    print()
    print(f"  Avg job time: {stats.avg_job_time:.3f}s")
    print(f"  Min job time: {stats.min_job_time:.3f}s")
    print(f"  Max job time: {stats.max_job_time:.3f}s")
    print()
    print(f"  Scale up events: {stats.scale_up_count}")
    print(f"  Scale down events: {stats.scale_down_count}")
    print("-" * 40)
    
    pool.stop(wait=True)
    scheduler.stop()


def example_graceful_shutdown() -> None:
    """Example 8: Graceful shutdown with in-flight jobs."""
    print("\n" + "=" * 60)
    print("Example 8: Graceful Shutdown")
    print("=" * 60)
    
    scheduler = Scheduler()
    scheduler.start()
    
    config = PoolConfig(
        min_workers=4,
        max_workers=8,
        worker_type=WorkerType.THREAD,
    )
    
    pool = WorkerPool(scheduler, config)
    pool.start()
    
    # Submit slow jobs
    print("\nSubmitting 8 slow jobs (2 seconds each)...")
    for i in range(8):
        job = Job(name=f"slow_{i}", func=slow_task)
        scheduler.submit(job)
    
    # Wait a bit for jobs to start
    time.sleep(0.5)
    
    stats = pool.get_stats()
    print(f"Jobs in progress: {stats.busy_workers}")
    
    # Graceful shutdown
    print("\nInitiating graceful shutdown (wait=True)...")
    print("(Workers will complete in-flight jobs)")
    
    start = time.time()
    pool.stop(wait=True, timeout=10.0)
    elapsed = time.time() - start
    
    print(f"\nShutdown completed in {elapsed:.1f}s")
    
    stats = pool.get_stats()
    print(f"Final jobs completed: {stats.jobs_completed}")
    
    scheduler.stop()


def example_configuration_tuning() -> None:
    """Example 9: Configuration tuning guidelines."""
    print("\n" + "=" * 60)
    print("Example 9: Configuration Tuning")
    print("=" * 60)
    
    print("""
Configuration Tuning Guidelines:

1. Worker Count:
   - I/O-bound: workers = CPU_count * 4-10
   - CPU-bound: workers = CPU_count
   - Mixed: workers = CPU_count * 2
   
2. Scale Thresholds:
   - Aggressive scaling: up=0.6, down=0.2
   - Conservative scaling: up=0.9, down=0.4
   - Stable (no scaling): up=1.0, down=0.0
   
3. Scale Interval:
   - Fast response: 2-5 seconds
   - Stable: 30-60 seconds
   - Cost-sensitive: 300+ seconds
   
4. Idle Timeout:
   - Cloud (cost): 60-300 seconds
   - On-premise: 300-3600 seconds
   
5. Health Checks:
   - Critical workloads: 2-5 seconds
   - Normal: 10-30 seconds
   - Relaxed: 60+ seconds
""")
    
    import multiprocessing
    cpu_count = multiprocessing.cpu_count()
    
    print(f"\nExample configurations for this system ({cpu_count} CPUs):")
    
    # I/O-bound configuration
    io_config = PoolConfig(
        min_workers=cpu_count,
        max_workers=cpu_count * 10,
        worker_type=WorkerType.THREAD,
        scale_up_threshold=0.7,
        scale_down_threshold=0.2,
        scale_interval=5.0,
    )
    print(f"\nI/O-bound workload:")
    print(f"  Workers: {io_config.min_workers}-{io_config.max_workers}")
    print(f"  Type: {io_config.worker_type.name}")
    
    # CPU-bound configuration
    cpu_config = PoolConfig(
        min_workers=cpu_count // 2,
        max_workers=cpu_count,
        worker_type=WorkerType.PROCESS,
        scale_up_threshold=0.8,
        scale_down_threshold=0.3,
        scale_interval=30.0,
    )
    print(f"\nCPU-bound workload:")
    print(f"  Workers: {cpu_config.min_workers}-{cpu_config.max_workers}")
    print(f"  Type: PROCESS (for true parallelism)")


def main() -> None:
    """Run all worker pool examples."""
    print("=" * 60)
    print("Job Orchestrator - Worker Pool Scaling Examples")
    print("=" * 60)
    
    example_basic_worker_pool()
    example_manual_scaling()
    example_pool_under_load()
    example_auto_scaling()
    example_worker_types()
    example_health_monitoring()
    example_pool_statistics()
    example_graceful_shutdown()
    example_configuration_tuning()
    
    print("\n" + "=" * 60)
    print("All worker pool examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()