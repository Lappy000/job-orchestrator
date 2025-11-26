"""
Tests for DAG (Directed Acyclic Graph) and DAGBuilder classes.

This module tests the DAG data structure, dependency management,
topological sorting, cycle detection, and the fluent DAGBuilder API.
"""

import pytest
from uuid import uuid4

from job_orchestrator import Job, JobState, JobPriority, DAG, DAGBuilder, DAGNode
from job_orchestrator.core.exceptions import CyclicDependencyError, DAGValidationError


class TestDAGCreation:
    """Tests for basic DAG creation and initialization."""

    def test_dag_creation_minimal(self):
        """Test creating a DAG with minimal arguments."""
        dag = DAG(name="test_dag")
        
        assert dag.name == "test_dag"
        assert len(dag.nodes) == 0
        assert len(dag.jobs) == 0
        assert dag.state == JobState.PENDING

    def test_dag_creation_with_description(self):
        """Test creating a DAG with description."""
        dag = DAG(name="test_dag", description="A test pipeline")
        
        assert dag.description == "A test pipeline"

    def test_dag_has_unique_id(self):
        """Test that each DAG has a unique ID."""
        dags = [DAG(name="test") for _ in range(10)]
        ids = [dag.id for dag in dags]
        
        assert len(set(ids)) == 10

    def test_dag_created_at_is_set(self):
        """Test that created_at timestamp is automatically set."""
        from datetime import datetime
        
        before = datetime.utcnow()
        dag = DAG(name="test")
        after = datetime.utcnow()
        
        assert before <= dag.created_at <= after


class TestDAGNodeManagement:
    """Tests for adding nodes and edges to DAG."""

    def test_add_node(self):
        """Test adding a node to the DAG."""
        dag = DAG(name="test")
        job = Job(name="job1")
        
        dag.add_node(job)
        
        assert job.id in dag.nodes
        assert job.id in dag.jobs
        assert dag.jobs[job.id] == job

    def test_add_edge(self):
        """Test adding an edge between two nodes."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_node(job_b)
        dag.add_edge(job_a.id, job_b.id)
        
        # job_b depends on job_a
        assert job_a.id in dag.nodes[job_b.id].dependencies
        assert job_b.id in dag.nodes[job_a.id].dependents

    def test_add_edge_missing_from_job(self):
        """Test adding edge with missing source job raises KeyError."""
        dag = DAG(name="test")
        job_b = Job(name="job_b")
        dag.add_node(job_b)
        
        with pytest.raises(KeyError):
            dag.add_edge(uuid4(), job_b.id)

    def test_add_edge_missing_to_job(self):
        """Test adding edge with missing target job raises KeyError."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        dag.add_node(job_a)
        
        with pytest.raises(KeyError):
            dag.add_edge(job_a.id, uuid4())

    def test_remove_edge(self):
        """Test removing an edge between nodes."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_node(job_b)
        dag.add_edge(job_a.id, job_b.id)
        dag.remove_edge(job_a.id, job_b.id)
        
        assert job_a.id not in dag.nodes[job_b.id].dependencies
        assert job_b.id not in dag.nodes[job_a.id].dependents

    def test_add_job_with_dependencies(self):
        """Test adding a job with dependencies."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_job(job_b, depends_on=[job_a.id])
        
        assert job_a.id in dag.nodes[job_b.id].dependencies


class TestDAGRootAndLeafNodes:
    """Tests for finding root and leaf nodes."""

    def test_get_root_nodes_single(self):
        """Test getting root nodes with single root."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_job(job_b, depends_on=[job_a.id])
        
        roots = dag.get_root_nodes()
        
        assert len(roots) == 1
        assert job_a.id in roots

    def test_get_root_nodes_multiple(self):
        """Test getting root nodes with multiple roots."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        job_c = Job(name="job_c")
        
        dag.add_node(job_a)
        dag.add_node(job_b)
        dag.add_job(job_c, depends_on=[job_a.id, job_b.id])
        
        roots = dag.get_root_nodes()
        
        assert len(roots) == 2
        assert job_a.id in roots
        assert job_b.id in roots

    def test_get_leaf_nodes_single(self):
        """Test getting leaf nodes with single leaf."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_job(job_b, depends_on=[job_a.id])
        
        leaves = dag.get_leaf_nodes()
        
        assert len(leaves) == 1
        assert job_b.id in leaves

    def test_get_leaf_nodes_multiple(self):
        """Test getting leaf nodes with multiple leaves."""
        dag = DAG(name="test")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        job_c = Job(name="job_c")
        
        dag.add_node(job_a)
        dag.add_job(job_b, depends_on=[job_a.id])
        dag.add_job(job_c, depends_on=[job_a.id])
        
        leaves = dag.get_leaf_nodes()
        
        assert len(leaves) == 2
        assert job_b.id in leaves
        assert job_c.id in leaves


class TestTopologicalSort:
    """Tests for topological sort using Kahn's algorithm."""

    def test_topological_sort_linear(self, simple_dag):
        """Test topological sort with linear dependencies."""
        sorted_jobs = simple_dag.topological_sort()
        
        # A should come before B, B before C
        names = [job.name for job in sorted_jobs]
        assert names.index("task_a") < names.index("task_b")
        assert names.index("task_b") < names.index("task_c")

    def test_topological_sort_parallel(self, parallel_dag):
        """Test topological sort with parallel jobs."""
        sorted_jobs = parallel_dag.topological_sort()
        
        names = [job.name for job in sorted_jobs]
        
        # A should come first
        assert names.index("task_a") < names.index("task_b")
        assert names.index("task_a") < names.index("task_c")
        
        # D should come last
        assert names.index("task_b") < names.index("task_d")
        assert names.index("task_c") < names.index("task_d")

    def test_topological_sort_diamond(self, diamond_dag):
        """Test topological sort with diamond pattern."""
        sorted_jobs = diamond_dag.topological_sort()
        
        names = [job.name for job in sorted_jobs]
        
        # A -> [B, C] -> D -> E
        assert names.index("task_a") < names.index("task_b")
        assert names.index("task_a") < names.index("task_c")
        assert names.index("task_b") < names.index("task_d")
        assert names.index("task_c") < names.index("task_d")
        assert names.index("task_d") < names.index("task_e")

    def test_topological_sort_empty_dag(self, empty_dag):
        """Test topological sort with empty DAG."""
        sorted_jobs = empty_dag.topological_sort()
        
        assert len(sorted_jobs) == 0

    def test_topological_sort_single_job(self, single_job_dag):
        """Test topological sort with single job."""
        sorted_jobs = single_job_dag.topological_sort()
        
        assert len(sorted_jobs) == 1


class TestCycleDetection:
    """Tests for cycle detection in DAGs."""

    def test_has_cycle_false(self, simple_dag):
        """Test has_cycle returns False for valid DAG."""
        assert simple_dag.has_cycle() is False

    def test_has_cycle_true(self):
        """Test has_cycle returns True when cycle exists."""
        dag = DAG(name="cyclic")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        job_c = Job(name="job_c")
        
        dag.add_node(job_a)
        dag.add_node(job_b)
        dag.add_node(job_c)
        
        # Create cycle: A -> B -> C -> A
        dag.add_edge(job_a.id, job_b.id)
        dag.add_edge(job_b.id, job_c.id)
        dag.add_edge(job_c.id, job_a.id)
        
        assert dag.has_cycle() is True

    def test_find_cycle(self):
        """Test find_cycle returns cycle path."""
        dag = DAG(name="cyclic")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_node(job_b)
        
        # Create cycle: A -> B -> A
        dag.add_edge(job_a.id, job_b.id)
        dag.add_edge(job_b.id, job_a.id)
        
        cycle = dag.find_cycle()
        
        assert cycle is not None
        assert len(cycle) >= 2

    def test_find_cycle_no_cycle(self, simple_dag):
        """Test find_cycle returns None when no cycle exists."""
        cycle = simple_dag.find_cycle()
        
        assert cycle is None

    def test_topological_sort_raises_on_cycle(self):
        """Test topological sort raises exception on cycle."""
        dag = DAG(name="cyclic")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_node(job_b)
        dag.add_edge(job_a.id, job_b.id)
        dag.add_edge(job_b.id, job_a.id)
        
        with pytest.raises(CyclicDependencyError):
            dag.topological_sort()


class TestGetReadyJobs:
    """Tests for finding jobs ready for execution."""

    def test_get_ready_jobs_initial(self, simple_dag):
        """Test get_ready_jobs returns root jobs initially."""
        ready = simple_dag.get_ready_jobs()
        
        # Only the first job (A) should be ready
        assert len(ready) == 1
        assert ready[0].name == "task_a"

    def test_get_ready_jobs_after_completion(self, simple_dag):
        """Test get_ready_jobs after completing a job."""
        ready = simple_dag.get_ready_jobs()
        first_job = ready[0]
        first_job.state = JobState.COMPLETED
        
        ready = simple_dag.get_ready_jobs()
        
        # Now job B should be ready
        assert len(ready) == 1
        assert ready[0].name == "task_b"

    def test_get_ready_jobs_parallel(self, parallel_dag):
        """Test get_ready_jobs with parallel jobs."""
        # Complete job A
        for job in parallel_dag.jobs.values():
            if job.name == "task_a":
                job.state = JobState.COMPLETED
                break
        
        ready = parallel_dag.get_ready_jobs()
        
        # B and C should both be ready
        assert len(ready) == 2
        names = {job.name for job in ready}
        assert names == {"task_b", "task_c"}

    def test_is_job_ready(self, simple_dag):
        """Test is_job_ready for specific job."""
        # Get job IDs
        job_ids = {
            simple_dag.jobs[jid].name: jid 
            for jid in simple_dag.jobs
        }
        
        assert simple_dag.is_job_ready(job_ids["task_a"]) is True
        assert simple_dag.is_job_ready(job_ids["task_b"]) is False
        
        # Complete A
        simple_dag.jobs[job_ids["task_a"]].state = JobState.COMPLETED
        
        assert simple_dag.is_job_ready(job_ids["task_b"]) is True


class TestGetExecutionPlan:
    """Tests for parallel execution level calculation."""

    def test_get_execution_plan_linear(self, simple_dag):
        """Test execution plan with linear dependencies."""
        plan = simple_dag.get_execution_plan()
        
        assert len(plan) == 3  # 3 levels, one per job
        assert len(plan[0]) == 1  # Level 0: A
        assert len(plan[1]) == 1  # Level 1: B
        assert len(plan[2]) == 1  # Level 2: C

    def test_get_execution_plan_parallel(self, parallel_dag):
        """Test execution plan with parallel jobs."""
        plan = parallel_dag.get_execution_plan()
        
        assert len(plan) == 3  # 3 levels
        assert len(plan[0]) == 1  # Level 0: A
        assert len(plan[1]) == 2  # Level 1: B and C (parallel)
        assert len(plan[2]) == 1  # Level 2: D

    def test_get_execution_plan_empty(self, empty_dag):
        """Test execution plan with empty DAG."""
        plan = empty_dag.get_execution_plan()
        
        assert len(plan) == 0

    def test_get_execution_plan_single_job(self, single_job_dag):
        """Test execution plan with single job."""
        plan = single_job_dag.get_execution_plan()
        
        assert len(plan) == 1
        assert len(plan[0]) == 1


class TestDAGValidation:
    """Tests for DAG validation."""

    def test_validate_valid_dag(self, simple_dag):
        """Test validate returns True for valid DAG."""
        assert simple_dag.validate() is True

    def test_validate_raises_on_cycle(self):
        """Test validate raises exception on cycle."""
        dag = DAG(name="cyclic")
        job_a = Job(name="job_a")
        job_b = Job(name="job_b")
        
        dag.add_node(job_a)
        dag.add_node(job_b)
        dag.add_edge(job_a.id, job_b.id)
        dag.add_edge(job_b.id, job_a.id)
        
        with pytest.raises(DAGValidationError):
            dag.validate()


class TestDAGProperties:
    """Tests for DAG properties."""

    def test_dag_len(self, simple_dag):
        """Test __len__ returns number of jobs."""
        assert len(simple_dag) == 3

    def test_dag_contains(self, simple_dag):
        """Test __contains__ for job ID."""
        job_id = list(simple_dag.jobs.keys())[0]
        
        assert job_id in simple_dag
        assert uuid4() not in simple_dag

    def test_dag_iter(self, simple_dag):
        """Test __iter__ yields jobs in topological order."""
        jobs = list(simple_dag)
        
        assert len(jobs) == 3
        names = [job.name for job in jobs]
        assert names.index("task_a") < names.index("task_b")
        assert names.index("task_b") < names.index("task_c")

    def test_dag_is_complete(self, simple_dag):
        """Test is_complete property."""
        assert simple_dag.is_complete is False
        
        for job in simple_dag.jobs.values():
            job.state = JobState.COMPLETED
        
        assert simple_dag.is_complete is True

    def test_dag_has_failed(self, simple_dag):
        """Test has_failed property."""
        assert simple_dag.has_failed is False
        
        list(simple_dag.jobs.values())[0].state = JobState.FAILED
        
        assert simple_dag.has_failed is True

    def test_dag_progress(self, simple_dag):
        """Test progress property."""
        assert simple_dag.progress == 0.0
        
        jobs = list(simple_dag.jobs.values())
        jobs[0].state = JobState.COMPLETED
        
        assert abs(simple_dag.progress - 1/3) < 0.01
        
        for job in jobs:
            job.state = JobState.COMPLETED
        
        assert simple_dag.progress == 1.0


class TestDAGNode:
    """Tests for DAGNode dataclass."""

    def test_dag_node_creation(self):
        """Test creating a DAGNode."""
        job_id = uuid4()
        node = DAGNode(job_id=job_id)
        
        assert node.job_id == job_id
        assert len(node.dependencies) == 0
        assert len(node.dependents) == 0

    def test_dag_node_add_dependency(self):
        """Test adding a dependency to a node."""
        node = DAGNode(job_id=uuid4())
        dep_id = uuid4()
        
        node.add_dependency(dep_id)
        
        assert dep_id in node.dependencies

    def test_dag_node_add_dependent(self):
        """Test adding a dependent to a node."""
        node = DAGNode(job_id=uuid4())
        dep_id = uuid4()
        
        node.add_dependent(dep_id)
        
        assert dep_id in node.dependents

    def test_dag_node_in_degree(self):
        """Test in_degree property."""
        node = DAGNode(job_id=uuid4())
        node.add_dependency(uuid4())
        node.add_dependency(uuid4())
        
        assert node.in_degree == 2

    def test_dag_node_out_degree(self):
        """Test out_degree property."""
        node = DAGNode(job_id=uuid4())
        node.add_dependent(uuid4())
        node.add_dependent(uuid4())
        node.add_dependent(uuid4())
        
        assert node.out_degree == 3

    def test_dag_node_is_root(self):
        """Test is_root property."""
        node = DAGNode(job_id=uuid4())
        
        assert node.is_root is True
        
        node.add_dependency(uuid4())
        
        assert node.is_root is False

    def test_dag_node_is_leaf(self):
        """Test is_leaf property."""
        node = DAGNode(job_id=uuid4())
        
        assert node.is_leaf is True
        
        node.add_dependent(uuid4())
        
        assert node.is_leaf is False


class TestDAGBuilder:
    """Tests for the fluent DAGBuilder API."""

    def test_dag_builder_basic(self):
        """Test basic DAGBuilder usage."""
        def task():
            return "result"
        
        dag = (DAGBuilder("test_pipeline")
            .add_job(task, job_id="a")
            .build())
        
        assert dag.name == "test_pipeline"
        assert len(dag.jobs) == 1

    def test_dag_builder_fluent_api(self):
        """Test DAGBuilder fluent interface chaining."""
        def task_a():
            return "A"
        
        def task_b():
            return "B"
        
        dag = (DAGBuilder("pipeline")
            .add_job(task_a, job_id="a")
            .add_job(task_b, job_id="b", depends_on=["a"])
            .with_fail_fast(True)
            .with_max_parallel(2)
            .with_metadata(version="1.0")
            .build())
        
        assert len(dag.jobs) == 2
        assert dag.fail_fast is True
        assert dag.max_parallel == 2
        assert dag.metadata["version"] == "1.0"

    def test_dag_builder_with_job_object(self):
        """Test adding Job objects to DAGBuilder."""
        job = Job(name="existing_job")
        
        dag = (DAGBuilder("pipeline")
            .add_job(job, job_id="a")
            .build())
        
        assert job.id in dag.jobs

    def test_dag_builder_parallel_jobs(self):
        """Test adding parallel jobs with DAGBuilder."""
        def task():
            return "done"
        
        dag = (DAGBuilder("parallel_pipeline")
            .add_job(task, job_id="root")
            .add_job(task, job_id="branch1", depends_on=["root"])
            .add_job(task, job_id="branch2", depends_on=["root"])
            .add_job(task, job_id="join", depends_on=["branch1", "branch2"])
            .build())
        
        # Check dependencies
        join_node = None
        for job_id, node in dag.nodes.items():
            if len(node.dependencies) == 2:
                join_node = node
                break
        
        assert join_node is not None
        assert join_node.in_degree == 2

    def test_dag_builder_missing_dependency_raises(self):
        """Test DAGBuilder raises on missing dependency."""
        def task():
            return "done"
        
        with pytest.raises(ValueError, match="not found"):
            (DAGBuilder("pipeline")
                .add_job(task, job_id="b", depends_on=["nonexistent"])
                .build())

    def test_dag_builder_with_rollback(self):
        """Test DAGBuilder with rollback configuration."""
        def rollback_handler():
            pass
        
        dag = (DAGBuilder("pipeline")
            .with_rollback(rollback_handler)
            .build_unchecked())
        
        assert dag.enable_rollback is True
        assert dag.rollback_handler is not None

    def test_dag_builder_build_unchecked(self):
        """Test build_unchecked skips validation."""
        def task():
            return "done"
        
        dag = (DAGBuilder("pipeline")
            .add_job(task, job_id="a")
            .build_unchecked())
        
        # Should succeed even without explicit validation
        assert len(dag.jobs) == 1

    def test_dag_builder_description(self):
        """Test DAGBuilder with description."""
        dag = (DAGBuilder("pipeline", description="Test pipeline")
            .build_unchecked())
        
        assert dag.description == "Test pipeline"