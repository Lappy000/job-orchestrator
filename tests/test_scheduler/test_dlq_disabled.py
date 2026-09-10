"""Public-API regression for permanent failures with the DLQ disabled."""

from job_orchestrator import (
    DAG,
    Job,
    JobState,
    OrchestratorConfig,
    RetryPolicy,
    Scheduler,
)


def test_disabled_dlq_keeps_failure_notifications_without_retaining_job():
    config = OrchestratorConfig()
    config.dlq.enabled = False
    config.validate()
    scheduler = Scheduler(config)
    scheduler.start()
    dlq_notifications, failure_notifications = [], []

    def fail():
        raise ValueError("deliberate local failure")

    job = Job(func=fail, retry_policy=RetryPolicy(max_retries=0))
    dag = DAG(name="disabled-dlq")
    dag.add_node(job)
    dag_id = scheduler.submit_dag(dag)
    scheduler.on_dlq_entry_added(dlq_notifications.append)
    scheduler.on_job_failed(lambda j, result: failure_notifications.append(result))
    try:
        assert scheduler.get_next_job(timeout=0.1) is job
        result = scheduler.run_job(job)
        assert result.success is False
        assert job.state == JobState.FAILED
        assert job.completed_at is not None
        assert job.completed_at == result.completed_at
        assert scheduler.get_dag_status(dag_id)["status"] == "failed"
        assert failure_notifications == [result]
        assert scheduler.get_stats()["jobs_failed"] == 1
        assert scheduler.get_stats()["dags_failed"] == 1
        assert scheduler.get_dlq_entries() == []
        assert dlq_notifications == []
        assert scheduler.get_stats()["jobs_sent_to_dlq"] == 0
    finally:
        scheduler.stop()
