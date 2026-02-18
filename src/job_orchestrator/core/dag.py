"""
DAG (Directed Acyclic Graph) implementation for the Job Orchestrator.

This module provides data structures for representing job dependencies
as a directed acyclic graph, along with a builder API for easy DAG construction.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Union
from uuid import UUID, uuid4

from .job import Job, JobState
from .exceptions import CyclicDependencyError, DAGValidationError


@dataclass
class DAGNode:
    """
    A node in the DAG representing a job with its dependencies.
    
    Each node contains a reference to a job and maintains lists of
    upstream (dependencies) and downstream (dependents) nodes.
    
    Attributes:
        job_id: The UUID of the job this node represents.
        dependencies: Set of job IDs this node depends on (upstream).
        dependents: Set of job IDs that depend on this node (downstream).
    """
    job_id: UUID
    dependencies: Set[UUID] = field(default_factory=set)
    dependents: Set[UUID] = field(default_factory=set)
    
    def add_dependency(self, job_id: UUID) -> None:
        """Add an upstream dependency."""
        self.dependencies.add(job_id)
    
    def add_dependent(self, job_id: UUID) -> None:
        """Add a downstream dependent."""
        self.dependents.add(job_id)
    
    def remove_dependency(self, job_id: UUID) -> None:
        """Remove an upstream dependency."""
        self.dependencies.discard(job_id)
    
    def remove_dependent(self, job_id: UUID) -> None:
        """Remove a downstream dependent."""
        self.dependents.discard(job_id)
    
    @property
    def in_degree(self) -> int:
        """Return the number of dependencies (in-degree)."""
        return len(self.dependencies)
    
    @property
    def out_degree(self) -> int:
        """Return the number of dependents (out-degree)."""
        return len(self.dependents)
    
    @property
    def is_root(self) -> bool:
        """Check if this is a root node (no dependencies)."""
        return len(self.dependencies) == 0
    
    @property
    def is_leaf(self) -> bool:
        """Check if this is a leaf node (no dependents)."""
        return len(self.dependents) == 0


@dataclass
class DAG:
    """
    Directed Acyclic Graph for job dependencies.
    
    Manages a collection of jobs with dependency relationships,
    providing methods for topological sorting, cycle detection,
    and finding jobs ready for execution.
    
    Attributes:
        id: Unique identifier for the DAG.
        name: Human-readable name for the DAG.
        description: Optional description of the DAG workflow.
        nodes: Dictionary mapping job IDs to DAGNodes.
        jobs: Dictionary mapping job IDs to Job objects.
        state: Current state of the DAG execution.
        created_at: When the DAG was created.
        started_at: When DAG execution began.
        completed_at: When DAG execution finished.
        fail_fast: If True, stop execution on first failure.
        max_parallel: Maximum number of jobs to run in parallel.
        enable_rollback: If True, enable transaction rollback on failure.
        rollback_handler: Custom function to call for rollback.
        metadata: Additional custom metadata.
    """
    
    # Identity
    id: UUID = field(default_factory=uuid4)
    name: str = ""
    description: str = ""
    
    # Graph structure
    nodes: Dict[UUID, DAGNode] = field(default_factory=dict)
    jobs: Dict[UUID, Job] = field(default_factory=dict)
    
    # State
    state: JobState = JobState.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Execution options
    fail_fast: bool = True
    max_parallel: Optional[int] = None
    
    # Transaction support
    enable_rollback: bool = False
    rollback_handler: Optional[Callable] = field(default=None, repr=False)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_node(self, job: Job) -> None:
        """
        Add a job to the DAG without any dependencies.
        
        Args:
            job: The job to add.
        """
        if job.id not in self.nodes:
            self.nodes[job.id] = DAGNode(job_id=job.id)
            self.jobs[job.id] = job
    
    def add_edge(self, from_job_id: UUID, to_job_id: UUID) -> None:
        """
        Add a dependency edge: to_job depends on from_job.
        
        Args:
            from_job_id: The upstream job ID (dependency).
            to_job_id: The downstream job ID (dependent).
            
        Raises:
            KeyError: If either job ID is not in the DAG.
        """
        if from_job_id not in self.nodes:
            raise KeyError(f"Job {from_job_id} not found in DAG")
        if to_job_id not in self.nodes:
            raise KeyError(f"Job {to_job_id} not found in DAG")
        
        # Add edge
        self.nodes[to_job_id].add_dependency(from_job_id)
        self.nodes[from_job_id].add_dependent(to_job_id)
        
        # Update job's depends_on list
        if from_job_id not in self.jobs[to_job_id].depends_on:
            self.jobs[to_job_id].depends_on.append(from_job_id)
    
    def remove_edge(self, from_job_id: UUID, to_job_id: UUID) -> None:
        """
        Remove a dependency edge.
        
        Args:
            from_job_id: The upstream job ID.
            to_job_id: The downstream job ID.
        """
        if from_job_id in self.nodes and to_job_id in self.nodes:
            self.nodes[to_job_id].remove_dependency(from_job_id)
            self.nodes[from_job_id].remove_dependent(to_job_id)
            
            # Update job's depends_on list
            if from_job_id in self.jobs[to_job_id].depends_on:
                self.jobs[to_job_id].depends_on.remove(from_job_id)
    
    def add_job(
        self,
        job: Job,
        depends_on: Optional[List[UUID]] = None
    ) -> None:
        """
        Add a job to the DAG with optional dependencies.
        
        Args:
            job: The job to add.
            depends_on: List of job IDs this job depends on.
        """
        self.add_node(job)
        
        if depends_on:
            for dep_id in depends_on:
                if dep_id in self.nodes:
                    self.add_edge(dep_id, job.id)
    
    def get_root_nodes(self) -> List[UUID]:
        """
        Get all root nodes (jobs with no dependencies).
        
        Returns:
            List of job IDs that have no dependencies.
        """
        return [
            node.job_id for node in self.nodes.values()
            if node.is_root
        ]
    
    def get_leaf_nodes(self) -> List[UUID]:
        """
        Get all leaf nodes (jobs with no dependents).
        
        Returns:
            List of job IDs that have no dependents.
        """
        return [
            node.job_id for node in self.nodes.values()
            if node.is_leaf
        ]
    
    def get_ready_jobs(self) -> List[Job]:
        """
        Get all jobs ready for execution.
        
        A job is ready if:
        - It is in PENDING state
        - All its dependencies are in COMPLETED state
        
        Returns:
            List of jobs ready for execution.
        """
        ready = []
        for job_id, node in self.nodes.items():
            job = self.jobs[job_id]
            
            if job.state != JobState.PENDING:
                continue
            
            # Check all dependencies are completed
            all_deps_completed = all(
                self.jobs[dep_id].state == JobState.COMPLETED
                for dep_id in node.dependencies
            )
            
            if all_deps_completed:
                ready.append(job)
        
        return ready
    
    def is_job_ready(self, job_id: UUID) -> bool:
        """
        Check if a specific job is ready for execution.
        
        Args:
            job_id: The job ID to check.
            
        Returns:
            True if the job is ready, False otherwise.
        """
        if job_id not in self.nodes:
            return False
        
        job = self.jobs[job_id]
        if job.state != JobState.PENDING:
            return False
        
        node = self.nodes[job_id]
        return all(
            self.jobs[dep_id].state == JobState.COMPLETED
            for dep_id in node.dependencies
        )
    
    def topological_sort(self) -> List[Job]:
        """
        Return jobs in topological order using Kahn's algorithm.
        
        This implements an iterative version of topological sort
        that's more efficient than recursive DFS for large graphs.
        
        Returns:
            List of jobs in topological order (dependencies first).
            
        Raises:
            CyclicDependencyError: If a cycle is detected.
        """
        # Calculate in-degrees
        in_degree: Dict[UUID, int] = {
            job_id: node.in_degree
            for job_id, node in self.nodes.items()
        }
        
        # Find all nodes with no incoming edges (use deque for O(1) popleft)
        queue = deque(job_id for job_id, degree in in_degree.items() if degree == 0)
        result: List[Job] = []
        
        while queue:
            job_id = queue.popleft()
            result.append(self.jobs[job_id])
            
            # Decrease in-degree of neighbors
            for dependent_id in self.nodes[job_id].dependents:
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    queue.append(dependent_id)
        
        # Check for cycle
        if len(result) != len(self.nodes):
            # Find nodes involved in cycle
            remaining = [
                str(job_id) for job_id, degree in in_degree.items()
                if degree > 0
            ]
            raise CyclicDependencyError(
                message="Cycle detected during topological sort",
                dag_id=self.id,
                cycle_path=remaining
            )
        
        return result
    
    def has_cycle(self) -> bool:
        """
        Check if the DAG contains a cycle.
        
        Uses DFS-based cycle detection.
        
        Returns:
            True if a cycle exists, False otherwise.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[UUID, int] = {job_id: WHITE for job_id in self.nodes}
        
        def dfs(job_id: UUID) -> bool:
            color[job_id] = GRAY
            
            for dependent_id in self.nodes[job_id].dependents:
                if color[dependent_id] == GRAY:
                    return True  # Back edge found - cycle
                if color[dependent_id] == WHITE:
                    if dfs(dependent_id):
                        return True
            
            color[job_id] = BLACK
            return False
        
        for job_id in self.nodes:
            if color[job_id] == WHITE:
                if dfs(job_id):
                    return True
        
        return False
    
    def find_cycle(self) -> Optional[List[UUID]]:
        """
        Find a cycle in the DAG if one exists.
        
        Returns:
            List of job IDs forming the cycle, or None if no cycle exists.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[UUID, int] = {job_id: WHITE for job_id in self.nodes}
        parent: Dict[UUID, Optional[UUID]] = {job_id: None for job_id in self.nodes}
        
        def dfs(job_id: UUID) -> Optional[UUID]:
            color[job_id] = GRAY
            
            for dependent_id in self.nodes[job_id].dependents:
                if color[dependent_id] == GRAY:
                    return dependent_id  # Cycle found
                if color[dependent_id] == WHITE:
                    parent[dependent_id] = job_id
                    result = dfs(dependent_id)
                    if result is not None:
                        return result
            
            color[job_id] = BLACK
            return None
        
        for job_id in self.nodes:
            if color[job_id] == WHITE:
                cycle_node = dfs(job_id)
                if cycle_node is not None:
                    # Reconstruct cycle
                    cycle = [cycle_node]
                    current = parent[cycle_node]
                    while current != cycle_node and current is not None:
                        cycle.append(current)
                        current = parent[current]
                    cycle.append(cycle_node)
                    return list(reversed(cycle))
        
        return None
    
    def validate(self) -> bool:
        """
        Validate the DAG has no cycles and all dependencies exist.
        
        Returns:
            True if the DAG is valid.
            
        Raises:
            DAGValidationError: If validation fails.
        """
        errors: List[str] = []
        
        # Check for missing dependencies
        for job_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                if dep_id not in self.nodes:
                    errors.append(
                        f"Job {job_id} has missing dependency: {dep_id}"
                    )
        
        # Check for cycles
        if self.has_cycle():
            cycle = self.find_cycle()
            cycle_str = " -> ".join(str(j) for j in (cycle or []))
            errors.append(f"Cycle detected: {cycle_str}")
        
        if errors:
            raise DAGValidationError(
                message="DAG validation failed",
                dag_id=self.id,
                errors=errors
            )
        
        return True
    
    def get_execution_plan(self) -> List[List[Job]]:
        """
        Get the execution plan as levels of parallelizable jobs.
        
        Jobs in each level can be executed in parallel, and all jobs
        in a level must complete before the next level can start.
        
        Returns:
            List of job lists, where jobs in each inner list can run in parallel.
        """
        if not self.nodes:
            return []
        
        levels: List[List[Job]] = []
        remaining = set(self.nodes.keys())
        in_degree = {
            job_id: self.nodes[job_id].in_degree
            for job_id in self.nodes
        }
        
        while remaining:
            # Find all nodes with in-degree 0
            current_level = []
            for job_id in list(remaining):
                if in_degree[job_id] == 0:
                    current_level.append(self.jobs[job_id])
                    remaining.remove(job_id)
            
            if not current_level:
                raise CyclicDependencyError(
                    message="Cycle detected while building execution plan",
                    dag_id=self.id
                )
            
            levels.append(current_level)
            
            # Decrease in-degree of dependent nodes
            for job in current_level:
                for dependent_id in self.nodes[job.id].dependents:
                    if dependent_id in remaining:
                        in_degree[dependent_id] -= 1
        
        return levels
    
    def __len__(self) -> int:
        """Return the number of jobs in the DAG."""
        return len(self.nodes)
    
    def __contains__(self, job_id: UUID) -> bool:
        """Check if a job ID is in the DAG."""
        return job_id in self.nodes
    
    def __iter__(self) -> Iterator[Job]:
        """Iterate over jobs in topological order."""
        return iter(self.topological_sort())
    
    @property
    def is_complete(self) -> bool:
        """Check if all jobs in the DAG are complete."""
        return all(
            job.state == JobState.COMPLETED
            for job in self.jobs.values()
        )
    
    @property
    def has_failed(self) -> bool:
        """Check if any job in the DAG has failed."""
        return any(
            job.state == JobState.FAILED
            for job in self.jobs.values()
        )
    
    @property
    def progress(self) -> float:
        """
        Get the completion progress as a percentage.
        
        Returns:
            Float between 0.0 and 1.0 representing completion.
        """
        if not self.jobs:
            return 1.0
        completed = sum(
            1 for job in self.jobs.values()
            if job.state == JobState.COMPLETED
        )
        return completed / len(self.jobs)


class DAGBuilder:
    """
    Fluent API for building DAGs.
    
    Provides a convenient builder pattern for constructing DAGs
    with jobs and dependencies.
    
    Example:
        >>> dag = (DAGBuilder("etl_pipeline")
        ...     .add_job(extract_func, job_id="extract")
        ...     .add_job(transform_func, job_id="transform", depends_on=["extract"])
        ...     .add_job(load_func, job_id="load", depends_on=["transform"])
        ...     .with_fail_fast(True)
        ...     .build())
    """
    
    def __init__(self, name: str, description: str = ""):
        """
        Initialize the DAG builder.
        
        Args:
            name: Name for the DAG.
            description: Optional description.
        """
        self.dag = DAG(name=name, description=description)
        self._job_ids: Dict[str, UUID] = {}
    
    def add_job(
        self,
        job_or_func: Union[Job, Callable],
        job_id: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        **job_kwargs
    ) -> "DAGBuilder":
        """
        Add a job to the DAG.
        
        Args:
            job_or_func: Either a Job instance or a callable function.
            job_id: String identifier for referencing in depends_on.
            depends_on: List of job_id strings this job depends on.
            **job_kwargs: Additional keyword arguments for Job creation
                         (only used if job_or_func is a callable).
        
        Returns:
            Self for method chaining.
        """
        if isinstance(job_or_func, Job):
            job = job_or_func
        elif callable(job_or_func):
            # Create job from callable
            from .job import JobPriority
            func_name = getattr(job_or_func, "__name__", "anonymous")
            func_path = f"{job_or_func.__module__}.{job_or_func.__qualname__}"
            
            job = Job(
                name=job_kwargs.get("name", func_name),
                func=job_or_func,
                func_path=func_path,
                args=job_kwargs.get("args", ()),
                kwargs=job_kwargs.get("kwargs", {}),
                priority=job_kwargs.get("priority", JobPriority.NORMAL),
                timeout=job_kwargs.get("timeout"),
                tags=job_kwargs.get("tags", {}),
            )
        else:
            raise TypeError(f"Expected Job or callable, got {type(job_or_func)}")
        
        # Store job ID mapping
        key = job_id or str(job.id)
        self._job_ids[key] = job.id
        
        # Add job to DAG
        self.dag.add_node(job)
        
        # Add dependencies
        if depends_on:
            for dep_key in depends_on:
                if dep_key not in self._job_ids:
                    raise ValueError(
                        f"Dependency '{dep_key}' not found. "
                        f"Jobs must be added before they can be depended on."
                    )
                self.dag.add_edge(self._job_ids[dep_key], job.id)
        
        return self
    
    def with_fail_fast(self, enabled: bool = True) -> "DAGBuilder":
        """
        Enable or disable fail-fast behavior.
        
        Args:
            enabled: If True, stop execution on first failure.
            
        Returns:
            Self for method chaining.
        """
        self.dag.fail_fast = enabled
        return self
    
    def with_max_parallel(self, limit: int) -> "DAGBuilder":
        """
        Set maximum parallel job execution.
        
        Args:
            limit: Maximum number of jobs to run in parallel.
            
        Returns:
            Self for method chaining.
        """
        self.dag.max_parallel = limit
        return self
    
    def with_rollback(
        self,
        handler: Optional[Callable] = None
    ) -> "DAGBuilder":
        """
        Enable transaction rollback with optional custom handler.
        
        Args:
            handler: Optional function to call for rollback.
            
        Returns:
            Self for method chaining.
        """
        self.dag.enable_rollback = True
        if handler:
            self.dag.rollback_handler = handler
        return self
    
    def with_metadata(self, **metadata) -> "DAGBuilder":
        """
        Add metadata to the DAG.
        
        Args:
            **metadata: Key-value pairs to add as metadata.
            
        Returns:
            Self for method chaining.
        """
        self.dag.metadata.update(metadata)
        return self
    
    def build(self) -> DAG:
        """
        Validate and return the built DAG.
        
        Returns:
            The constructed DAG.
            
        Raises:
            DAGValidationError: If the DAG is invalid.
        """
        self.dag.validate()
        return self.dag
    
    def build_unchecked(self) -> DAG:
        """
        Return the DAG without validation.
        
        Use with caution - the DAG may contain cycles or invalid references.
        
        Returns:
            The constructed DAG without validation.
        """
        return self.dag


__all__ = [
    "DAGNode",
    "DAG",
    "DAGBuilder",
]