"""Real-thread regressions for the pool lifecycle boundary."""

import pytest

from job_orchestrator import Scheduler
from job_orchestrator.workers.pool import PoolConfig, WorkerPool


@pytest.mark.parametrize("started_before", [False, True])
def test_scale_up_does_not_spawn_workers_in_an_inactive_pool(started_before):
    scheduler = Scheduler()
    scheduler.start()
    pool = WorkerPool(
        scheduler,
        PoolConfig(min_workers=0, max_workers=1),
    )
    try:
        if started_before:
            pool.start()
            pool.stop(wait=True, timeout=2)

        added = pool.scale_up(1)

        assert added == 0
        assert pool.worker_count == 0
        assert pool.is_running is False
    finally:
        # RED must also clean up workers created by the old implementation.
        # pool.stop() alone returns early when its running flag is false.
        for info in pool.get_worker_info():
            worker = pool.get_worker(info.worker_id)
            if worker is not None:
                worker.stop(wait=True, timeout=2)
        pool.stop(wait=False, timeout=1)
        scheduler.stop(wait=False)
