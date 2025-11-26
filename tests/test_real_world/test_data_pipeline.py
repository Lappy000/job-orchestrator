"""
Data Pipeline Processing Tests
==============================

Scenario: A data engineering team downloads job-orchestrator to build
their data processing pipelines. They need to:

1. Extract data from multiple sources (databases, APIs, files)
2. Transform and clean data
3. Aggregate and join datasets
4. Load into data warehouse
5. Generate reports
6. Handle failures with proper rollback

This test suite verifies these data engineering requirements.
"""

import pytest
import time
import random
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from decimal import Decimal
import json

from job_orchestrator import (
    Job,
    JobState,
    JobPriority,
    DAG,
    DAGBuilder,
    Scheduler,
    OrchestratorConfig,
    ThreadSafePriorityQueue,
)
from job_orchestrator.core.job import RetryPolicy
from job_orchestrator.core.config import WorkerPoolConfig, RetryConfig


# =============================================================================
# Data Models (simulating real data engineering entities)
# =============================================================================

@dataclass
class DataRecord:
    """Generic data record."""
    id: str
    source: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    is_valid: bool = True
    validation_errors: List[str] = field(default_factory=list)


@dataclass
class Dataset:
    """Collection of data records."""
    name: str
    records: List[DataRecord] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def count(self) -> int:
        return len(self.records)
    
    def add(self, record: DataRecord) -> None:
        self.records.append(record)
    
    def filter_valid(self) -> "Dataset":
        return Dataset(
            name=f"{self.name}_valid",
            records=[r for r in self.records if r.is_valid],
            metadata={**self.metadata, "filtered": True}
        )


@dataclass
class PipelineContext:
    """Context for tracking pipeline execution."""
    pipeline_id: str
    start_time: datetime
    stages: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    checkpoints: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Mock Data Sources
# =============================================================================

class MockDatabaseSource:
    """Simulates a database data source."""
    
    def __init__(self, failure_rate: float = 0.0):
        self.failure_rate = failure_rate
        self.query_count = 0
        self._lock = threading.Lock()
    
    def query(self, table: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Execute a query and return records."""
        with self._lock:
            self.query_count += 1
        
        if random.random() < self.failure_rate:
            raise ConnectionError(f"Database connection lost for table {table}")
        
        # Simulate query time
        time.sleep(0.02)
        
        # Generate mock data
        records = []
        for i in range(min(limit, 100)):
            records.append({
                "id": f"{table}_{i:04d}",
                "name": f"Record {i}",
                "value": random.uniform(0, 1000),
                "category": random.choice(["A", "B", "C"]),
                "created_at": datetime.utcnow().isoformat(),
            })
        
        return records


class MockAPISource:
    """Simulates an API data source."""
    
    def __init__(self, rate_limit: int = 100):
        self.rate_limit = rate_limit
        self.call_count = 0
        self._lock = threading.Lock()
    
    def fetch(self, endpoint: str, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """Fetch data from API endpoint."""
        with self._lock:
            self.call_count += 1
            if self.call_count > self.rate_limit:
                raise Exception("API rate limit exceeded")
        
        # Simulate API latency
        time.sleep(0.03)
        
        # Generate mock response
        return {
            "endpoint": endpoint,
            "page": page,
            "total_pages": 5,
            "data": [
                {
                    "id": f"api_{page}_{i:03d}",
                    "type": endpoint.split("/")[-1],
                    "attributes": {
                        "score": random.uniform(0, 100),
                        "active": random.choice([True, False]),
                    },
                }
                for i in range(page_size)
            ]
        }


class MockFileSource:
    """Simulates file-based data source."""
    
    def __init__(self):
        self._files = {
            "users.csv": self._generate_csv_data("users", 50),
            "products.json": self._generate_json_data("products", 30),
            "orders.parquet": self._generate_csv_data("orders", 100),
        }
    
    def _generate_csv_data(self, name: str, count: int) -> List[Dict]:
        return [
            {"id": f"{name}_{i}", "name": f"{name.title()} {i}", "value": i * 10}
            for i in range(count)
        ]
    
    def _generate_json_data(self, name: str, count: int) -> List[Dict]:
        return [
            {
                "id": f"{name}_{i}",
                "properties": {"category": random.choice(["X", "Y", "Z"])},
            }
            for i in range(count)
        ]
    
    def read(self, filename: str) -> List[Dict[str, Any]]:
        """Read file and return data."""
        time.sleep(0.01)  # Simulate I/O
        
        if filename not in self._files:
            raise FileNotFoundError(f"File not found: {filename}")
        
        return self._files[filename]


class MockDataWarehouse:
    """Simulates a data warehouse destination."""
    
    def __init__(self):
        self.tables: Dict[str, List[Dict]] = {}
        self._lock = threading.Lock()
        self.write_count = 0
    
    def create_table(self, table_name: str, schema: Dict = None) -> bool:
        """Create a new table."""
        with self._lock:
            if table_name not in self.tables:
                self.tables[table_name] = []
            return True
    
    def insert(self, table_name: str, records: List[Dict]) -> int:
        """Insert records into a table."""
        with self._lock:
            if table_name not in self.tables:
                self.tables[table_name] = []
            
            self.tables[table_name].extend(records)
            self.write_count += len(records)
            
            return len(records)
    
    def query(self, table_name: str) -> List[Dict]:
        """Query all records from a table."""
        with self._lock:
            return self.tables.get(table_name, [])
    
    def truncate(self, table_name: str) -> bool:
        """Truncate a table."""
        with self._lock:
            if table_name in self.tables:
                self.tables[table_name] = []
            return True


# =============================================================================
# Data Pipeline Implementation
# =============================================================================

class DataPipeline:
    """
    Data processing pipeline built with job-orchestrator.
    
    This represents what a data engineering team would build.
    """
    
    def __init__(self):
        self.config = OrchestratorConfig.from_dict({
            "worker_pool": {
                "min_workers": 2,
                "max_workers": 6,
            },
            "retry": {
                "max_retries": 3,
                "base_delay": 0.1,
                "exponential_base": 2.0,
            },
        })
        
        self.scheduler = Scheduler(self.config)
        
        # Data sources
        self.db_source = MockDatabaseSource()
        self.api_source = MockAPISource()
        self.file_source = MockFileSource()
        
        # Destination
        self.warehouse = MockDataWarehouse()
        
        # Pipeline state
        self.datasets: Dict[str, Dataset] = {}
        self.context: Optional[PipelineContext] = None
        self._lock = threading.Lock()
    
    def start(self):
        self.scheduler.start()
    
    def stop(self):
        self.scheduler.stop()
    
    def create_context(self, pipeline_id: str) -> PipelineContext:
        """Create a new pipeline context."""
        ctx = PipelineContext(
            pipeline_id=pipeline_id,
            start_time=datetime.utcnow(),
        )
        self.context = ctx
        return ctx
    
    def run_etl_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Run complete ETL pipeline.
        
        Pipeline structure:
        
        extract_db ─┐
                    ├─► transform ─► validate ─► aggregate ─► load ─► report
        extract_api ┘
        """
        ctx = self.create_context(pipeline_id)
        
        try:
            # Stage 1: Extract from multiple sources (parallel in real world)
            ctx.stages.append("extract_db")
            db_data = self.extract_from_database("sales", limit=50)
            
            ctx.stages.append("extract_api")
            api_data = self.extract_from_api("/v1/customers", pages=2)
            
            ctx.stages.append("extract_files")
            file_data = self.extract_from_file("users.csv")
            
            ctx.checkpoints["extraction_complete"] = datetime.utcnow().isoformat()
            
            # Stage 2: Transform - clean and normalize data
            ctx.stages.append("transform")
            all_records = db_data + api_data + file_data
            transformed = self.transform_records(all_records)
            
            ctx.checkpoints["transformation_complete"] = datetime.utcnow().isoformat()
            
            # Stage 3: Validate - check data quality
            ctx.stages.append("validate")
            valid_records, invalid_records = self.validate_records(transformed)
            
            ctx.metrics["valid_count"] = len(valid_records)
            ctx.metrics["invalid_count"] = len(invalid_records)
            
            ctx.checkpoints["validation_complete"] = datetime.utcnow().isoformat()
            
            # Stage 4: Aggregate - compute metrics
            ctx.stages.append("aggregate")
            aggregated = self.aggregate_records(valid_records)
            
            ctx.checkpoints["aggregation_complete"] = datetime.utcnow().isoformat()
            
            # Stage 5: Load - write to warehouse
            ctx.stages.append("load")
            rows_loaded = self.load_to_warehouse(aggregated, "analytics_data")
            
            ctx.metrics["rows_loaded"] = rows_loaded
            ctx.checkpoints["load_complete"] = datetime.utcnow().isoformat()
            
            # Stage 6: Generate report
            ctx.stages.append("report")
            report = self.generate_report(ctx)
            
            return {
                "success": True,
                "pipeline_id": pipeline_id,
                "stages_completed": ctx.stages,
                "metrics": ctx.metrics,
                "report": report,
            }
            
        except Exception as e:
            ctx.errors.append(str(e))
            return {
                "success": False,
                "pipeline_id": pipeline_id,
                "stages_completed": ctx.stages,
                "error": str(e),
            }
    
    def extract_from_database(self, table: str, limit: int = 100) -> List[Dict]:
        """Extract data from database."""
        records = self.db_source.query(table, limit)
        
        with self._lock:
            self.datasets[f"db_{table}"] = Dataset(
                name=f"db_{table}",
                records=[
                    DataRecord(id=r["id"], source="database", data=r)
                    for r in records
                ]
            )
        
        return records
    
    def extract_from_api(self, endpoint: str, pages: int = 1) -> List[Dict]:
        """Extract data from API."""
        all_records = []
        
        for page in range(1, pages + 1):
            response = self.api_source.fetch(endpoint, page)
            all_records.extend(response["data"])
        
        with self._lock:
            self.datasets[f"api_{endpoint}"] = Dataset(
                name=f"api_{endpoint}",
                records=[
                    DataRecord(id=r["id"], source="api", data=r)
                    for r in all_records
                ]
            )
        
        return all_records
    
    def extract_from_file(self, filename: str) -> List[Dict]:
        """Extract data from file."""
        records = self.file_source.read(filename)
        
        with self._lock:
            self.datasets[f"file_{filename}"] = Dataset(
                name=f"file_{filename}",
                records=[
                    DataRecord(id=r.get("id", str(i)), source="file", data=r)
                    for i, r in enumerate(records)
                ]
            )
        
        return records
    
    def transform_records(self, records: List[Dict]) -> List[Dict]:
        """Transform and normalize records."""
        transformed = []
        
        for record in records:
            # Normalize structure
            transformed_record = {
                "id": record.get("id"),
                "source_type": record.get("type", "unknown"),
                "value": float(record.get("value", 0)),
                "category": record.get("category", record.get("attributes", {}).get("category", "N/A")),
                "processed_at": datetime.utcnow().isoformat(),
            }
            transformed.append(transformed_record)
        
        return transformed
    
    def validate_records(self, records: List[Dict]) -> tuple:
        """Validate records and separate valid from invalid."""
        valid = []
        invalid = []
        
        for record in records:
            errors = []
            
            # Validation rules
            if not record.get("id"):
                errors.append("Missing ID")
            if record.get("value", 0) < 0:
                errors.append("Negative value")
            if record.get("category") == "N/A":
                errors.append("Missing category")
            
            if errors:
                record["validation_errors"] = errors
                invalid.append(record)
            else:
                valid.append(record)
        
        return valid, invalid
    
    def aggregate_records(self, records: List[Dict]) -> Dict[str, Any]:
        """Aggregate records and compute metrics."""
        if not records:
            return {"categories": {}, "total_value": 0, "count": 0}
        
        # Group by category
        by_category = {}
        total_value = 0
        
        for record in records:
            cat = record.get("category", "unknown")
            if cat not in by_category:
                by_category[cat] = {"count": 0, "total_value": 0, "records": []}
            
            by_category[cat]["count"] += 1
            by_category[cat]["total_value"] += record.get("value", 0)
            by_category[cat]["records"].append(record["id"])
            
            total_value += record.get("value", 0)
        
        return {
            "categories": by_category,
            "total_value": total_value,
            "count": len(records),
            "avg_value": total_value / len(records),
        }
    
    def load_to_warehouse(self, data: Dict[str, Any], table_name: str) -> int:
        """Load aggregated data to warehouse."""
        self.warehouse.create_table(table_name)
        
        # Flatten for loading
        records_to_load = []
        
        for category, cat_data in data.get("categories", {}).items():
            records_to_load.append({
                "category": category,
                "count": cat_data["count"],
                "total_value": cat_data["total_value"],
                "loaded_at": datetime.utcnow().isoformat(),
            })
        
        # Add summary record
        records_to_load.append({
            "category": "_summary",
            "count": data.get("count", 0),
            "total_value": data.get("total_value", 0),
            "avg_value": data.get("avg_value", 0),
            "loaded_at": datetime.utcnow().isoformat(),
        })
        
        return self.warehouse.insert(table_name, records_to_load)
    
    def generate_report(self, ctx: PipelineContext) -> Dict[str, Any]:
        """Generate pipeline execution report."""
        return {
            "pipeline_id": ctx.pipeline_id,
            "start_time": ctx.start_time.isoformat(),
            "end_time": datetime.utcnow().isoformat(),
            "duration_seconds": (datetime.utcnow() - ctx.start_time).total_seconds(),
            "stages": ctx.stages,
            "metrics": ctx.metrics,
            "checkpoints": ctx.checkpoints,
            "errors": ctx.errors,
            "status": "completed" if not ctx.errors else "completed_with_errors",
        }


# =============================================================================
# Test Classes
# =============================================================================

class TestDataPipelineBasics:
    """Basic tests for data pipeline functionality."""
    
    @pytest.fixture
    def pipeline(self):
        """Create and start a data pipeline."""
        pipeline = DataPipeline()
        pipeline.start()
        yield pipeline
        pipeline.stop()
    
    def test_extract_from_database(self, pipeline):
        """Test extracting data from database source."""
        records = pipeline.extract_from_database("customers", limit=25)
        
        assert len(records) == 25
        assert all("id" in r for r in records)
        assert "db_customers" in pipeline.datasets
    
    def test_extract_from_api(self, pipeline):
        """Test extracting data from API source."""
        records = pipeline.extract_from_api("/v1/products", pages=2)
        
        assert len(records) == 100  # 50 per page * 2 pages
        assert all("id" in r for r in records)
    
    def test_extract_from_file(self, pipeline):
        """Test extracting data from file source."""
        records = pipeline.extract_from_file("users.csv")
        
        assert len(records) == 50
        assert all("id" in r for r in records)
    
    def test_transform_records(self, pipeline):
        """Test record transformation."""
        raw_records = [
            {"id": "1", "value": 100, "category": "A"},
            {"id": "2", "value": 200, "type": "product"},
        ]
        
        transformed = pipeline.transform_records(raw_records)
        
        assert len(transformed) == 2
        assert all("processed_at" in r for r in transformed)
        assert transformed[0]["category"] == "A"
    
    def test_validate_records(self, pipeline):
        """Test record validation."""
        records = [
            {"id": "1", "value": 100, "category": "A"},  # Valid
            {"id": None, "value": 100, "category": "B"},  # Invalid - no ID
            {"id": "3", "value": -50, "category": "C"},  # Invalid - negative value
        ]
        
        valid, invalid = pipeline.validate_records(records)
        
        assert len(valid) == 1
        assert len(invalid) == 2
        assert valid[0]["id"] == "1"
    
    def test_aggregate_records(self, pipeline):
        """Test record aggregation."""
        records = [
            {"id": "1", "value": 100, "category": "A"},
            {"id": "2", "value": 200, "category": "A"},
            {"id": "3", "value": 150, "category": "B"},
        ]
        
        aggregated = pipeline.aggregate_records(records)
        
        assert aggregated["count"] == 3
        assert aggregated["total_value"] == 450
        assert "categories" in aggregated
        assert aggregated["categories"]["A"]["count"] == 2
        assert aggregated["categories"]["B"]["count"] == 1
    
    def test_load_to_warehouse(self, pipeline):
        """Test loading data to warehouse."""
        data = {
            "categories": {
                "A": {"count": 10, "total_value": 1000, "records": []},
                "B": {"count": 5, "total_value": 500, "records": []},
            },
            "count": 15,
            "total_value": 1500,
            "avg_value": 100,
        }
        
        rows_loaded = pipeline.load_to_warehouse(data, "test_table")
        
        assert rows_loaded == 3  # 2 categories + 1 summary
        assert len(pipeline.warehouse.query("test_table")) == 3


class TestCompletePipeline:
    """Tests for complete ETL pipeline execution."""
    
    @pytest.fixture
    def pipeline(self):
        """Create and start a data pipeline."""
        pipeline = DataPipeline()
        pipeline.start()
        yield pipeline
        pipeline.stop()
    
    def test_run_complete_etl_pipeline(self, pipeline):
        """Test running the complete ETL pipeline."""
        result = pipeline.run_etl_pipeline("test_pipeline_001")
        
        assert result["success"] is True
        assert result["pipeline_id"] == "test_pipeline_001"
        
        # All stages should be completed
        expected_stages = [
            "extract_db", "extract_api", "extract_files",
            "transform", "validate", "aggregate", "load", "report"
        ]
        assert result["stages_completed"] == expected_stages
        
        # Metrics should be populated
        assert "valid_count" in result["metrics"]
        assert "rows_loaded" in result["metrics"]
        assert result["metrics"]["rows_loaded"] > 0
    
    def test_pipeline_generates_report(self, pipeline):
        """Test pipeline generates a proper report."""
        result = pipeline.run_etl_pipeline("report_test")
        
        assert "report" in result
        report = result["report"]
        
        assert report["pipeline_id"] == "report_test"
        assert "start_time" in report
        assert "end_time" in report
        assert "duration_seconds" in report
        assert report["duration_seconds"] >= 0
        assert report["status"] == "completed"
    
    def test_pipeline_tracks_checkpoints(self, pipeline):
        """Test pipeline tracks checkpoints for recovery."""
        result = pipeline.run_etl_pipeline("checkpoint_test")
        
        checkpoints = pipeline.context.checkpoints
        
        assert "extraction_complete" in checkpoints
        assert "transformation_complete" in checkpoints
        assert "validation_complete" in checkpoints
        assert "aggregation_complete" in checkpoints
        assert "load_complete" in checkpoints
    
    def test_pipeline_loads_data_to_warehouse(self, pipeline):
        """Test data is actually loaded to warehouse."""
        pipeline.run_etl_pipeline("warehouse_test")
        
        # Check warehouse has data
        data = pipeline.warehouse.query("analytics_data")
        
        assert len(data) > 0
        assert any(r.get("category") == "_summary" for r in data)
    
    def test_multiple_pipeline_runs(self, pipeline):
        """Test running multiple pipelines."""
        results = []
        
        for i in range(3):
            result = pipeline.run_etl_pipeline(f"multi_run_{i}")
            results.append(result)
        
        assert all(r["success"] for r in results)
        assert all(r["pipeline_id"] == f"multi_run_{i}" for i, r in enumerate(results))


class TestPipelineWithDAG:
    """Tests for DAG-based pipeline execution."""
    
    @pytest.fixture
    def scheduler(self):
        """Create a scheduler for DAG testing."""
        scheduler = Scheduler()
        scheduler.start()
        yield scheduler
        scheduler.stop()
    
    def test_parallel_extraction_dag(self, scheduler):
        """Test DAG with parallel extraction stages."""
        extracted_sources = []
        lock = threading.Lock()
        
        def extract_source(name):
            def task():
                time.sleep(0.02)
                with lock:
                    extracted_sources.append(name)
                return {name: "extracted"}
            return task
        
        def merge_data():
            return {"merged": len(extracted_sources)}
        
        # Build DAG with parallel extraction
        dag = (DAGBuilder("parallel_extract", "Parallel extraction pipeline")
            .add_job(extract_source("database"), job_id="extract_db")
            .add_job(extract_source("api"), job_id="extract_api")
            .add_job(extract_source("files"), job_id="extract_files")
            .add_job(merge_data, job_id="merge",
                    depends_on=["extract_db", "extract_api", "extract_files"])
            .build())
        
        # Execute
        for job in dag.topological_sort():
            scheduler.run_job(job)
        
        # All sources should be extracted
        assert set(extracted_sources) == {"database", "api", "files"}
    
    def test_etl_dag_with_validation(self, scheduler):
        """Test ETL DAG with validation stage."""
        stages = []
        data_store = {"records": []}
        
        def extract():
            stages.append("extract")
            data_store["records"] = [
                {"id": "1", "value": 100},
                {"id": "2", "value": -50},  # Invalid
                {"id": "3", "value": 200},
            ]
            return {"count": 3}
        
        def validate():
            stages.append("validate")
            valid = [r for r in data_store["records"] if r["value"] >= 0]
            data_store["valid_records"] = valid
            return {"valid": len(valid), "invalid": 1}
        
        def load():
            stages.append("load")
            return {"loaded": len(data_store.get("valid_records", []))}
        
        dag = (DAGBuilder("etl_validate", "ETL with validation")
            .add_job(extract, job_id="extract")
            .add_job(validate, job_id="validate", depends_on=["extract"])
            .add_job(load, job_id="load", depends_on=["validate"])
            .build())
        
        # Execute
        for job in dag.topological_sort():
            result = scheduler.run_job(job)
            assert result.success
        
        assert stages == ["extract", "validate", "load"]
        assert len(data_store["valid_records"]) == 2


class TestPipelineResilience:
    """Tests for pipeline error handling and resilience."""
    
    @pytest.fixture
    def scheduler(self):
        """Create a scheduler with retry config."""
        config = OrchestratorConfig.from_dict({
            "retry": {
                "max_retries": 3,
                "base_delay": 0.01,
            },
        })
        scheduler = Scheduler(config)
        scheduler.start()
        yield scheduler
        scheduler.stop()
    
    def test_extraction_retry_on_failure(self, scheduler):
        """Test extraction retries on connection failure."""
        attempt_count = [0]
        
        def flaky_extract():
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ConnectionError("Database connection lost")
            return {"data": [1, 2, 3]}
        
        job = Job(
            name="flaky_extract",
            func=flaky_extract,
            retry_policy=RetryPolicy(max_retries=5, base_delay=0.01),
        )
        
        scheduler.submit(job)
        
        # Run multiple times to allow retries
        for _ in range(3):
            result = scheduler.run_job(job)
            if result.success:
                break
        
        # Should eventually succeed
        assert attempt_count[0] >= 3
    
    def test_pipeline_handles_missing_file(self):
        """Test pipeline handles missing file gracefully."""
        pipeline = DataPipeline()
        pipeline.start()
        
        try:
            with pytest.raises(FileNotFoundError):
                pipeline.extract_from_file("nonexistent.csv")
        finally:
            pipeline.stop()
    
    def test_pipeline_handles_api_rate_limit(self):
        """Test pipeline handles API rate limiting."""
        pipeline = DataPipeline()
        pipeline.api_source.rate_limit = 5  # Very low limit
        pipeline.start()
        
        try:
            # Should fail after exceeding rate limit
            with pytest.raises(Exception, match="rate limit"):
                for _ in range(10):
                    pipeline.extract_from_api("/v1/customers", pages=1)
        finally:
            pipeline.stop()
    
    def test_empty_dataset_handling(self):
        """Test pipeline handles empty datasets."""
        pipeline = DataPipeline()
        pipeline.start()
        
        try:
            # Transform empty list
            transformed = pipeline.transform_records([])
            assert transformed == []
            
            # Validate empty list
            valid, invalid = pipeline.validate_records([])
            assert valid == []
            assert invalid == []
            
            # Aggregate empty list
            aggregated = pipeline.aggregate_records([])
            assert aggregated["count"] == 0
            assert aggregated["total_value"] == 0
            
        finally:
            pipeline.stop()


class TestDataQuality:
    """Tests for data quality validation."""
    
    @pytest.fixture
    def pipeline(self):
        """Create and start a data pipeline."""
        pipeline = DataPipeline()
        pipeline.start()
        yield pipeline
        pipeline.stop()
    
    def test_validation_catches_null_ids(self, pipeline):
        """Test validation catches null IDs."""
        records = [{"id": None, "value": 100, "category": "A"}]
        
        valid, invalid = pipeline.validate_records(records)
        
        assert len(valid) == 0
        assert len(invalid) == 1
        assert "Missing ID" in invalid[0]["validation_errors"]
    
    def test_validation_catches_negative_values(self, pipeline):
        """Test validation catches negative values."""
        records = [{"id": "1", "value": -100, "category": "A"}]
        
        valid, invalid = pipeline.validate_records(records)
        
        assert len(valid) == 0
        assert len(invalid) == 1
        assert "Negative value" in invalid[0]["validation_errors"]
    
    def test_validation_catches_missing_category(self, pipeline):
        """Test validation catches missing category."""
        records = [{"id": "1", "value": 100, "category": "N/A"}]
        
        valid, invalid = pipeline.validate_records(records)
        
        assert len(valid) == 0
        assert len(invalid) == 1
        assert "Missing category" in invalid[0]["validation_errors"]
    
    def test_validation_multiple_errors(self, pipeline):
        """Test validation catches multiple errors in one record."""
        records = [{"id": None, "value": -50, "category": "N/A"}]
        
        valid, invalid = pipeline.validate_records(records)
        
        assert len(invalid) == 1
        assert len(invalid[0]["validation_errors"]) == 3


class TestPipelineMetrics:
    """Tests for pipeline metrics and monitoring."""
    
    @pytest.fixture
    def pipeline(self):
        """Create and start a data pipeline."""
        pipeline = DataPipeline()
        pipeline.start()
        yield pipeline
        pipeline.stop()
    
    def test_pipeline_tracks_record_counts(self, pipeline):
        """Test pipeline tracks record counts through stages."""
        result = pipeline.run_etl_pipeline("metrics_test")
        
        assert "valid_count" in result["metrics"]
        assert "invalid_count" in result["metrics"]
        assert "rows_loaded" in result["metrics"]
        
        # Valid + invalid should equal total processed
        total = result["metrics"]["valid_count"] + result["metrics"]["invalid_count"]
        assert total > 0
    
    def test_pipeline_tracks_source_queries(self, pipeline):
        """Test pipeline tracks source query counts."""
        pipeline.run_etl_pipeline("query_test")
        
        assert pipeline.db_source.query_count >= 1
        assert pipeline.api_source.call_count >= 1
    
    def test_pipeline_tracks_warehouse_writes(self, pipeline):
        """Test pipeline tracks warehouse write counts."""
        pipeline.run_etl_pipeline("warehouse_write_test")
        
        assert pipeline.warehouse.write_count > 0


class TestRealWorldDataScenarios:
    """Real-world data processing scenarios."""
    
    def test_daily_sales_aggregation(self):
        """
        Simulate daily sales data aggregation.
        
        A common task for data teams - aggregate daily sales
        by category and region.
        """
        scheduler = Scheduler()
        scheduler.start()
        
        try:
            sales_data = []
            lock = threading.Lock()
            
            def extract_sales():
                # Simulate extracting sales from multiple sources
                return [
                    {"id": i, "amount": random.uniform(10, 500), 
                     "category": random.choice(["Electronics", "Clothing", "Food"]),
                     "region": random.choice(["North", "South", "East", "West"])}
                    for i in range(100)
                ]
            
            def aggregate_by_category(sales):
                by_cat = {}
                for sale in sales:
                    cat = sale["category"]
                    if cat not in by_cat:
                        by_cat[cat] = {"count": 0, "total": 0}
                    by_cat[cat]["count"] += 1
                    by_cat[cat]["total"] += sale["amount"]
                return by_cat
            
            def aggregate_by_region(sales):
                by_region = {}
                for sale in sales:
                    region = sale["region"]
                    if region not in by_region:
                        by_region[region] = {"count": 0, "total": 0}
                    by_region[region]["count"] += 1
                    by_region[region]["total"] += sale["amount"]
                return by_region
            
            # Extract
            extract_job = Job(name="extract_sales", func=extract_sales)
            scheduler.submit(extract_job)
            extract_result = scheduler.run_job(extract_job)
            
            assert extract_result.success
            sales = extract_result.result
            assert len(sales) == 100
            
            # Aggregate in parallel
            cat_job = Job(
                name="agg_category",
                func=lambda: aggregate_by_category(sales)
            )
            region_job = Job(
                name="agg_region",
                func=lambda: aggregate_by_region(sales)
            )
            
            scheduler.submit(cat_job)
            scheduler.submit(region_job)
            
            cat_result = scheduler.run_job(cat_job)
            region_result = scheduler.run_job(region_job)
            
            assert cat_result.success
            assert region_result.success
            
            # Verify results
            cat_agg = cat_result.result
            region_agg = region_result.result
            
            assert sum(c["count"] for c in cat_agg.values()) == 100
            assert sum(r["count"] for r in region_agg.values()) == 100
            
        finally:
            scheduler.stop()
    
    def test_customer_analytics_pipeline(self):
        """
        Simulate customer analytics pipeline.
        
        Compute customer lifetime value and segment customers.
        """
        scheduler = Scheduler()
        scheduler.start()
        
        try:
            # Mock customer data
            customers = [
                {
                    "id": f"cust_{i}",
                    "total_orders": random.randint(1, 50),
                    "total_spent": random.uniform(100, 10000),
                    "days_since_first_order": random.randint(30, 730),
                }
                for i in range(50)
            ]
            
            def calculate_ltv(customer):
                """Calculate customer lifetime value."""
                avg_order_value = customer["total_spent"] / max(customer["total_orders"], 1)
                orders_per_year = customer["total_orders"] / (customer["days_since_first_order"] / 365)
                return avg_order_value * orders_per_year * 3  # 3-year projection
            
            def segment_customers(customers):
                """Segment customers by LTV."""
                segments = {"platinum": [], "gold": [], "silver": [], "bronze": []}
                
                for cust in customers:
                    ltv = calculate_ltv(cust)
                    cust["ltv"] = ltv
                    
                    if ltv > 5000:
                        segments["platinum"].append(cust)
                    elif ltv > 2000:
                        segments["gold"].append(cust)
                    elif ltv > 500:
                        segments["silver"].append(cust)
                    else:
                        segments["bronze"].append(cust)
                
                return {k: len(v) for k, v in segments.items()}
            
            job = Job(
                name="customer_segmentation",
                func=lambda: segment_customers(customers)
            )
            
            scheduler.submit(job)
            result = scheduler.run_job(job)
            
            assert result.success
            segments = result.result
            
            # All customers should be segmented
            assert sum(segments.values()) == 50
            
        finally:
            scheduler.stop()
    
    def test_log_processing_pipeline(self):
        """
        Simulate log processing pipeline.
        
        Parse, filter, and aggregate application logs.
        """
        scheduler = Scheduler()
        scheduler.start()
        
        try:
            # Mock log entries
            log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            logs = [
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": random.choice(log_levels),
                    "message": f"Log message {i}",
                    "service": random.choice(["api", "web", "worker", "scheduler"]),
                }
                for i in range(200)
            ]
            
            def filter_errors(logs):
                """Filter to only error and critical logs."""
                return [l for l in logs if l["level"] in ["ERROR", "CRITICAL"]]
            
            def aggregate_by_service(logs):
                """Aggregate errors by service."""
                by_service = {}
                for log in logs:
                    svc = log["service"]
                    if svc not in by_service:
                        by_service[svc] = 0
                    by_service[svc] += 1
                return by_service
            
            # Build DAG
            dag = (DAGBuilder("log_processing", "Log processing pipeline")
                .add_job(lambda: filter_errors(logs), job_id="filter")
                .add_job(lambda: aggregate_by_service(filter_errors(logs)), 
                        job_id="aggregate", depends_on=["filter"])
                .build())
            
            # Execute
            for job in dag.topological_sort():
                result = scheduler.run_job(job)
                assert result.success
            
        finally:
            scheduler.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])