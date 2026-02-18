"""
Dead Letter Queue implementation for the Job Orchestrator.

This module provides a dead letter queue for permanently failed jobs.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional
from uuid import UUID, uuid4
import threading
import logging

from ..core.job import Job, JobState


logger = logging.getLogger(__name__)


class DLQEntryStatus(Enum):
    """Status of a DLQ entry."""

    PENDING = "pending"
    REQUEUED = "requeued"
    DISCARDED = "discarded"
    RESOLVED = "resolved"


@dataclass
class DLQStats:
    """Statistics for the Dead Letter Queue."""

    total: int = 0
    pending: int = 0
    requeued: int = 0
    discarded: int = 0
    resolved: int = 0
    oldest_entry: Optional[datetime] = None
    newest_entry: Optional[datetime] = None
    by_error_type: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            "total": self.total,
            "pending": self.pending,
            "requeued": self.requeued,
            "discarded": self.discarded,
            "resolved": self.resolved,
            "oldest_entry": self.oldest_entry.isoformat() if self.oldest_entry else None,
            "newest_entry": self.newest_entry.isoformat() if self.newest_entry else None,
            "by_error_type": self.by_error_type,
        }


@dataclass
class DLQEntry:
    """A single record inside the dead letter queue."""

    entry_id: str
    job: Job
    job_id: str
    job_name: str
    error_type: str
    error_message: str
    stack_trace: Optional[str] = None
    reason: str = ""
    added_at: datetime = field(default_factory=datetime.utcnow)
    status: DLQEntryStatus = DLQEntryStatus.PENDING
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    notes: str = ""
    retry_count: int = 0
    resolved: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "job_id": self.job_id,
            "job_name": self.job_name,
            "status": self.status.value,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "reason": self.reason,
            "retry_count": self.retry_count,
            "added_at": self.added_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolved_by": self.resolved_by,
            "notes": self.notes,
            "resolved": self.resolved,
        }

    @property
    def error(self) -> str:
        """Backwards compatible accessor used by older code paths."""
        return self.error_message


class DeadLetterQueue:
    """Dead letter queue for permanently failed jobs."""

    def __init__(
        self,
        *,
        max_entries: Optional[int] = None,
        max_size: Optional[int] = None,
        ttl: Optional[float] = None,
        ttl_days: Optional[int] = None,
        auto_cleanup: bool = False,
        cleanup_interval: float = 3600.0,
    ) -> None:
        self.max_entries = max_entries or max_size
        self.ttl = ttl if ttl is not None else (ttl_days * 86400.0 if ttl_days else None)
        self.auto_cleanup = auto_cleanup
        self._cleanup_interval = cleanup_interval
        self._entries: Dict[str, DLQEntry] = {}
        self._job_index: Dict[str, str] = {}
        self._callbacks: List[Callable[[DLQEntry], None]] = []
        self._lock = threading.RLock()
        self._shutdown_event = threading.Event()
        self._cleanup_thread: Optional[threading.Thread] = None
        self._ttl_seconds = self.ttl

        if self._ttl_seconds and cleanup_interval > 0:
            self._start_cleanup_thread()

    # ------------------------------------------------------------------
    # Thread helpers
    # ------------------------------------------------------------------
    def _start_cleanup_thread(self) -> None:
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="dlq-cleanup",
        )
        self._cleanup_thread.start()

    def _cleanup_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                self._shutdown_event.wait(self._cleanup_interval)
                if self._shutdown_event.is_set():
                    break
                self.cleanup_expired()
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("Error in DLQ cleanup thread: %s", exc, exc_info=True)

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------
    def add(
        self,
        job: Job,
        error: Optional[Exception] = None,
        traceback: Optional[str] = None,
        retry_count: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> str:
        """Store a failed job into the DLQ."""

        with self._lock:
            if self.max_entries and len(self._entries) >= self.max_entries:
                self._evict_oldest()

            job_id = str(job.id)
            error_obj = error or job.error
            error_type = type(error_obj).__name__ if error_obj else "Unknown"
            error_message = str(error_obj) if error_obj else ""
            stack = traceback or getattr(job, "traceback", None)

            entry_id = str(uuid4())
            entry = DLQEntry(
                entry_id=entry_id,
                job=job,
                job_id=job_id,
                job_name=job.name,
                error_type=error_type,
                error_message=error_message,
                stack_trace=stack,
                reason=reason or "",
                retry_count=retry_count if retry_count is not None else getattr(job, "retry_count", 0),
            )

            self._entries[entry_id] = entry
            self._job_index[job_id] = entry_id

            logger.warning("Job %s added to DLQ (entry %s): %s", job_id, entry_id, error_message)

            for callback in self._callbacks:
                try:
                    callback(entry)
                except Exception as exc:  # pragma: no cover - defensive
                    logger.error("Error in DLQ callback: %s", exc, exc_info=True)

            if self.auto_cleanup:
                self.cleanup_expired()

            return entry_id

    def requeue(
        self,
        identifier: str,
        scheduler: Optional[Any] = None,
        reset_retry_count: bool = True,
        resolved_by: Optional[str] = None,
        modifier: Optional[Callable[[Job], Job]] = None,
    ) -> Optional[Any]:
        """Requeue a job from the DLQ.

        When ``scheduler`` is provided the job is resubmitted immediately and
        ``True``/``False`` is returned. Without a scheduler the job is returned
        to the caller for manual handling.
        """

        with self._lock:
            entry = self._get_entry(identifier)
            if not entry:
                return None if scheduler is None else False

            job = entry.job
            if reset_retry_count:
                job.retry_count = 0
            job.state = JobState.PENDING
            job.error = None
            job.traceback = None

            if modifier:
                job = modifier(job)
                entry.job = job

            entry.status = DLQEntryStatus.REQUEUED
            entry.resolved = True
            entry.resolved_at = datetime.utcnow()
            entry.resolved_by = resolved_by

            self._remove_entry(entry.entry_id)

        if scheduler is None:
            return job

        try:
            scheduler.submit(job)
            return True
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Failed to requeue job %s: %s", job.id, exc, exc_info=True)
            with self._lock:
                self._register_entry(entry)
            return False

    def discard(
        self,
        identifier: str,
        notes: str = "",
        resolved_by: Optional[str] = None,
    ) -> bool:
        with self._lock:
            entry = self._get_entry(identifier)
            if not entry:
                return False

            entry.status = DLQEntryStatus.DISCARDED
            entry.resolved = True
            entry.resolved_at = datetime.utcnow()
            entry.resolved_by = resolved_by
            entry.notes = notes
            self._remove_entry(entry.entry_id)
            return True

    def resolve(
        self,
        identifier: str,
        notes: str = "",
        resolved_by: Optional[str] = None,
        keep_in_history: bool = False,
    ) -> bool:
        with self._lock:
            entry = self._get_entry(identifier)
            if not entry:
                return False

            entry.status = DLQEntryStatus.RESOLVED
            entry.resolved = True
            entry.resolved_at = datetime.utcnow()
            entry.resolved_by = resolved_by
            entry.notes = notes

            if not keep_in_history:
                self._remove_entry(entry.entry_id)

            return True

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def get(self, entry_id: str) -> Optional[DLQEntry]:
        with self._lock:
            return self._entries.get(entry_id)

    def get_entry(self, identifier: str) -> Optional[DLQEntry]:
        with self._lock:
            return self._get_entry(identifier)

    def get_all(
        self,
        status: Optional[DLQEntryStatus] = None,
        limit: Optional[int] = 100,
        offset: int = 0,
    ) -> List[DLQEntry]:
        with self._lock:
            entries: Iterable[DLQEntry] = self._entries.values()
            if status:
                entries = [entry for entry in entries if entry.status == status]

            entries = sorted(entries, key=lambda e: e.added_at, reverse=True)
            if limit is None:
                return list(entries)[offset:]
            return list(entries)[offset : offset + limit]

    def filter_by_error_type(self, error_type: str) -> List[DLQEntry]:
        with self._lock:
            return [entry for entry in self._entries.values() if entry.error_type == error_type]

    def filter_by_time(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[DLQEntry]:
        with self._lock:
            results: List[DLQEntry] = []
            for entry in self._entries.values():
                if start and entry.added_at < start:
                    continue
                if end and entry.added_at > end:
                    continue
                results.append(entry)
            return results

    def get_stats(self) -> DLQStats:
        with self._lock:
            entries = list(self._entries.values())
            by_status = {
                DLQEntryStatus.PENDING: 0,
                DLQEntryStatus.REQUEUED: 0,
                DLQEntryStatus.DISCARDED: 0,
                DLQEntryStatus.RESOLVED: 0,
            }
            by_error: Dict[str, int] = {}

            for entry in entries:
                by_status[entry.status] += 1
                by_error[entry.error_type] = by_error.get(entry.error_type, 0) + 1

            oldest = min(entries, key=lambda e: e.added_at).added_at if entries else None
            newest = max(entries, key=lambda e: e.added_at).added_at if entries else None

            return DLQStats(
                total=len(entries),
                pending=by_status[DLQEntryStatus.PENDING],
                requeued=by_status[DLQEntryStatus.REQUEUED],
                discarded=by_status[DLQEntryStatus.DISCARDED],
                resolved=by_status[DLQEntryStatus.RESOLVED],
                oldest_entry=oldest,
                newest_entry=newest,
                by_error_type=by_error,
            )

    def get_failure_analytics(self) -> Dict[str, Any]:
        with self._lock:
            entries = list(self._entries.values())
            if not entries:
                return {
                    "total_failures": 0,
                    "most_common_errors": [],
                    "failure_rate_trend": [],
                }

            error_counts: Dict[str, int] = {}
            for entry in entries:
                error_counts[entry.error_type] = error_counts.get(entry.error_type, 0) + 1

            most_common = sorted(error_counts.items(), key=lambda item: item[1], reverse=True)
            return {
                "total_failures": len(entries),
                "most_common_errors": most_common,
                "failure_rate_trend": [],
            }

    def get_failure_patterns(self) -> List[Dict[str, Any]]:
        with self._lock:
            patterns: Dict[str, int] = {}
            for entry in self._entries.values():
                key = f"{entry.job_name}:{entry.error_type}"
                patterns[key] = patterns.get(key, 0) + 1
            return [
                {"pattern": key, "count": count}
                for key, count in sorted(patterns.items(), key=lambda item: item[1], reverse=True)
            ]

    def get_most_common_errors(self, limit: int = 5) -> List[Dict[str, Any]]:
        analytics = self.get_failure_analytics()
        return [
            {"error": error, "count": count}
            for error, count in analytics["most_common_errors"][:limit]
        ]

    def get_failure_rate(self, period: timedelta) -> float:
        if period.total_seconds() <= 0:
            return 0.0
        cutoff = datetime.utcnow() - period
        with self._lock:
            recent = sum(1 for entry in self._entries.values() if entry.added_at >= cutoff)
        return recent / period.total_seconds()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def cleanup_expired(self) -> int:
        if not self._ttl_seconds:
            return 0

        cutoff = datetime.utcnow() - timedelta(seconds=self._ttl_seconds)
        removed = 0
        with self._lock:
            for entry_id, entry in list(self._entries.items()):
                if entry.added_at < cutoff:
                    self._remove_entry(entry_id)
                    removed += 1

        if removed:
            logger.info("Cleaned up %s expired DLQ entries", removed)
        return removed

    def shutdown(self) -> None:
        self._shutdown_event.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5.0)

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def _evict_oldest(self) -> None:
        if not self._entries:
            return
        oldest_id = min(self._entries, key=lambda key: self._entries[key].added_at)
        self._remove_entry(oldest_id)

    def _register_entry(self, entry: DLQEntry) -> None:
        self._entries[entry.entry_id] = entry
        self._job_index[entry.job_id] = entry.entry_id

    def _remove_entry(self, entry_id: str) -> None:
        entry = self._entries.pop(entry_id, None)
        if entry:
            self._job_index.pop(entry.job_id, None)

    def _get_entry(self, identifier: str) -> Optional[DLQEntry]:
        entry = self._entries.get(identifier)
        if entry:
            return entry
        entry_id = self._job_index.get(identifier)
        if entry_id:
            return self._entries.get(entry_id)
        return None

    # ------------------------------------------------------------------
    # Container protocol helpers
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def __iter__(self):
        with self._lock:
            return iter(list(self._entries.values()))

    def __contains__(self, identifier: object) -> bool:  # type: ignore[override]
        if not isinstance(identifier, (str, UUID)):
            return False
        return self.get_entry(str(identifier)) is not None

    @property
    def is_empty(self) -> bool:
        return len(self) == 0

    def on_entry_added(self, callback: Callable[[DLQEntry], None]) -> None:
        self._callbacks.append(callback)

    def __repr__(self) -> str:
        return f"DeadLetterQueue(entries={len(self)})"


__all__ = [
    "DLQEntry",
    "DLQEntryStatus",
    "DLQStats",
    "DeadLetterQueue",
]
