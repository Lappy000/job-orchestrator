"""
Main CLI entry point for the Job Orchestrator.

Provides subcommands for job management, scheduler control, and
status inspection. Uses argparse for zero external dependencies.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..core.config import OrchestratorConfig
from ..core.job import Job, JobPriority, JobState


def _get_scheduler(config_path: Optional[str] = None):
    """Initialize scheduler from config file or defaults."""
    from ..scheduler import Scheduler

    if config_path:
        if config_path.endswith((".yml", ".yaml")):
            config = OrchestratorConfig.from_yaml(config_path)
        elif config_path.endswith(".toml"):
            config = OrchestratorConfig.from_toml(config_path)
        else:
            raise ValueError(f"Unsupported config format: {config_path}")
    else:
        config = OrchestratorConfig.from_env()

    return Scheduler(config)


def _format_table(headers: List[str], rows: List[List[str]], max_width: int = 120) -> str:
    """Format data as an aligned text table."""
    if not rows:
        return "No results."

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    # Truncate columns if total width exceeds max
    total = sum(col_widths) + (len(headers) - 1) * 3
    if total > max_width:
        # Shrink the widest column
        widest_idx = col_widths.index(max(col_widths))
        col_widths[widest_idx] -= total - max_width

    # Header line
    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(headers)))

    lines = [header_line, separator]
    for row in rows:
        line = " | ".join(
            str(cell).ljust(col_widths[i])[:col_widths[i]] for i, cell in enumerate(row)
        )
        lines.append(line)

    return "\n".join(lines)


def _format_json(data: Any) -> str:
    """Format data as indented JSON."""
    return json.dumps(data, indent=2, default=str)


def cmd_status(args: argparse.Namespace) -> int:
    """Display scheduler status and statistics."""
    scheduler = _get_scheduler(args.config)
    scheduler.start()

    stats = scheduler.get_stats()
    scheduler.stop()

    if args.json:
        print(_format_json(stats))
    else:
        print("=== Job Orchestrator Status ===")
        print(f"  Running:          {stats.get('is_running', False)}")
        print(f"  Jobs submitted:   {stats.get('jobs_submitted', 0)}")
        print(f"  Jobs completed:   {stats.get('jobs_completed', 0)}")
        print(f"  Jobs failed:      {stats.get('jobs_failed', 0)}")
        print(f"  Jobs retried:     {stats.get('jobs_retried', 0)}")
        print(f"  Jobs in DLQ:      {stats.get('jobs_sent_to_dlq', 0)}")
        print(f"  DAGs submitted:   {stats.get('dags_submitted', 0)}")
        print(f"  DAGs completed:   {stats.get('dags_completed', 0)}")
        print(f"  DAGs failed:      {stats.get('dags_failed', 0)}")
        print()

        queue_stats = stats.get("queue", {})
        if queue_stats:
            print("=== Queue ===")
            print(f"  Size:             {queue_stats.get('size', 0)}")
            print(f"  Max size:         {queue_stats.get('max_size', 'unlimited')}")
            print(f"  Total pushed:     {queue_stats.get('total_pushed', 0)}")
            print(f"  Total popped:     {queue_stats.get('total_popped', 0)}")
            print()

        dlq_stats = stats.get("dlq", {})
        if dlq_stats:
            print("=== Dead Letter Queue ===")
            print(f"  Pending:          {dlq_stats.get('pending_count', 0)}")
            print(f"  Resolved:         {dlq_stats.get('resolved_count', 0)}")
            print(f"  Discarded:        {dlq_stats.get('discarded_count', 0)}")
            print(f"  Total:            {dlq_stats.get('total_count', 0)}")

    return 0


def cmd_list_jobs(args: argparse.Namespace) -> int:
    """List jobs with optional state filter."""
    scheduler = _get_scheduler(args.config)
    scheduler.start()

    state_filter = None
    if args.state:
        try:
            state_filter = JobState(args.state)
        except ValueError:
            print(f"Error: Invalid state '{args.state}'. Valid states: "
                  f"{', '.join(s.value for s in JobState)}", file=sys.stderr)
            return 1

    jobs = scheduler.list_jobs(state=state_filter, limit=args.limit, offset=args.offset)
    scheduler.stop()

    if args.json:
        job_dicts = []
        for job in jobs:
            job_dicts.append({
                "id": str(job.id),
                "name": job.name,
                "state": job.state.value,
                "priority": job.priority.name,
                "attempt": job.attempt,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            })
        print(_format_json(job_dicts))
    else:
        headers = ["ID", "Name", "State", "Priority", "Attempt", "Created"]
        rows = []
        for job in jobs:
            created = job.created_at.strftime("%Y-%m-%d %H:%M:%S") if job.created_at else "-"
            rows.append([
                str(job.id)[:8],
                job.name[:30],
                job.state.value,
                job.priority.name,
                str(job.attempt),
                created,
            ])
        print(_format_table(headers, rows))

    return 0


def cmd_inspect_job(args: argparse.Namespace) -> int:
    """Show detailed information about a specific job."""
    scheduler = _get_scheduler(args.config)
    scheduler.start()

    job = scheduler.get_job(args.job_id)
    scheduler.stop()

    if not job:
        print(f"Error: Job '{args.job_id}' not found.", file=sys.stderr)
        return 1

    data = {
        "id": str(job.id),
        "name": job.name,
        "state": job.state.value,
        "priority": job.priority.name,
        "attempt": job.attempt,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if hasattr(job, 'started_at') and job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "result": str(job.result) if job.result else None,
        "error": job.error if hasattr(job, 'error') else None,
        "traceback": job.traceback if hasattr(job, 'traceback') else None,
    }

    if args.json:
        print(_format_json(data))
    else:
        print(f"=== Job: {data['name']} ===")
        for key, value in data.items():
            if value is not None:
                print(f"  {key:15s}: {value}")

    return 0


def cmd_cancel_job(args: argparse.Namespace) -> int:
    """Cancel a pending or scheduled job."""
    scheduler = _get_scheduler(args.config)
    scheduler.start()

    try:
        result = scheduler.cancel_job(args.job_id)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        scheduler.stop()
        return 1

    scheduler.stop()

    if result:
        print(f"Job '{args.job_id}' cancelled.")
    else:
        print(f"Could not cancel job '{args.job_id}' (may already be running or completed).",
              file=sys.stderr)
        return 1

    return 0


def cmd_dlq_list(args: argparse.Namespace) -> int:
    """List entries in the dead letter queue."""
    from ..scheduler import DLQEntryStatus

    scheduler = _get_scheduler(args.config)
    scheduler.start()

    status_filter = None
    if args.status:
        try:
            status_filter = DLQEntryStatus(args.status)
        except ValueError:
            print(f"Error: Invalid status '{args.status}'. Valid statuses: "
                  f"{', '.join(s.value for s in DLQEntryStatus)}", file=sys.stderr)
            return 1

    entries = scheduler.get_dlq_entries(status=status_filter, limit=args.limit)
    scheduler.stop()

    if args.json:
        entry_dicts = []
        for entry in entries:
            entry_dicts.append({
                "entry_id": entry.entry_id,
                "job_name": entry.job.name,
                "job_id": str(entry.job.id),
                "status": entry.status.value,
                "error": entry.original_error,
                "added_at": entry.added_at.isoformat() if entry.added_at else None,
                "retry_count": len(entry.retry_history) if hasattr(entry, 'retry_history') else 0,
            })
        print(_format_json(entry_dicts))
    else:
        headers = ["Entry ID", "Job Name", "Status", "Error", "Added"]
        rows = []
        for entry in entries:
            added = entry.added_at.strftime("%Y-%m-%d %H:%M") if entry.added_at else "-"
            error_str = str(entry.original_error)[:40] if entry.original_error else "-"
            rows.append([
                entry.entry_id[:8],
                entry.job.name[:25],
                entry.status.value,
                error_str,
                added,
            ])
        print(_format_table(headers, rows))

    return 0


def cmd_dlq_requeue(args: argparse.Namespace) -> int:
    """Requeue a DLQ entry for retry."""
    scheduler = _get_scheduler(args.config)
    scheduler.start()

    result = scheduler.requeue_dlq_entry(
        args.entry_id,
        reset_retry_count=not args.keep_retries
    )
    scheduler.stop()

    if result:
        print(f"Entry '{args.entry_id}' requeued for processing.")
    else:
        print(f"Failed to requeue entry '{args.entry_id}'.", file=sys.stderr)
        return 1

    return 0


def cmd_dlq_discard(args: argparse.Namespace) -> int:
    """Discard a DLQ entry permanently."""
    scheduler = _get_scheduler(args.config)
    scheduler.start()

    result = scheduler.discard_dlq_entry(args.entry_id, notes=args.reason or "")
    scheduler.stop()

    if result:
        print(f"Entry '{args.entry_id}' discarded.")
    else:
        print(f"Failed to discard entry '{args.entry_id}'.", file=sys.stderr)
        return 1

    return 0


def cmd_config_validate(args: argparse.Namespace) -> int:
    """Validate a configuration file."""
    try:
        if args.file.endswith((".yml", ".yaml")):
            config = OrchestratorConfig.from_yaml(args.file)
        elif args.file.endswith(".toml"):
            config = OrchestratorConfig.from_toml(args.file)
        else:
            print(f"Error: Unsupported config format: {args.file}", file=sys.stderr)
            return 1

        config.validate()
        print(f"Configuration '{args.file}' is valid.")
        return 0

    except FileNotFoundError:
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Validation error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        return 1


def cmd_config_show(args: argparse.Namespace) -> int:
    """Show current effective configuration."""
    if args.config:
        if args.config.endswith((".yml", ".yaml")):
            config = OrchestratorConfig.from_yaml(args.config)
        elif args.config.endswith(".toml"):
            config = OrchestratorConfig.from_toml(args.config)
        else:
            config = OrchestratorConfig.from_env()
    else:
        config = OrchestratorConfig.from_env()

    import dataclasses

    def _to_dict(obj: Any) -> Any:
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return {k: _to_dict(v) for k, v in dataclasses.asdict(obj).items()}
        return obj

    print(_format_json(_to_dict(config)))
    return 0


def cmd_version(args: argparse.Namespace) -> int:
    """Show version information."""
    from .. import __version__
    print(f"job-orchestrator {__version__}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="job-orch",
        description="Job Orchestrator CLI - manage jobs, DAGs, and scheduler operations",
    )
    parser.add_argument(
        "--config", "-c",
        help="Path to configuration file (YAML or TOML)",
        default=None,
    )
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        help="Output in JSON format",
        default=False,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- status ---
    status_parser = subparsers.add_parser("status", help="Show scheduler status and statistics")
    status_parser.set_defaults(func=cmd_status)

    # --- version ---
    version_parser = subparsers.add_parser("version", help="Show version information")
    version_parser.set_defaults(func=cmd_version)

    # --- jobs ---
    jobs_parser = subparsers.add_parser("jobs", help="Job management commands")
    jobs_sub = jobs_parser.add_subparsers(dest="jobs_command")

    # jobs list
    jobs_list = jobs_sub.add_parser("list", help="List jobs")
    jobs_list.add_argument("--state", "-s", help="Filter by state")
    jobs_list.add_argument("--limit", "-l", type=int, default=50, help="Max results (default: 50)")
    jobs_list.add_argument("--offset", "-o", type=int, default=0, help="Offset for pagination")
    jobs_list.set_defaults(func=cmd_list_jobs)

    # jobs inspect
    jobs_inspect = jobs_sub.add_parser("inspect", help="Show detailed job info")
    jobs_inspect.add_argument("job_id", help="Job ID to inspect")
    jobs_inspect.set_defaults(func=cmd_inspect_job)

    # jobs cancel
    jobs_cancel = jobs_sub.add_parser("cancel", help="Cancel a job")
    jobs_cancel.add_argument("job_id", help="Job ID to cancel")
    jobs_cancel.set_defaults(func=cmd_cancel_job)

    # --- dlq ---
    dlq_parser = subparsers.add_parser("dlq", help="Dead letter queue management")
    dlq_sub = dlq_parser.add_subparsers(dest="dlq_command")

    # dlq list
    dlq_list = dlq_sub.add_parser("list", help="List DLQ entries")
    dlq_list.add_argument("--status", "-s", help="Filter by status")
    dlq_list.add_argument("--limit", "-l", type=int, default=50, help="Max results")
    dlq_list.set_defaults(func=cmd_dlq_list)

    # dlq requeue
    dlq_requeue = dlq_sub.add_parser("requeue", help="Requeue a DLQ entry")
    dlq_requeue.add_argument("entry_id", help="DLQ entry ID to requeue")
    dlq_requeue.add_argument("--keep-retries", action="store_true",
                             help="Keep existing retry count")
    dlq_requeue.set_defaults(func=cmd_dlq_requeue)

    # dlq discard
    dlq_discard = dlq_sub.add_parser("discard", help="Discard a DLQ entry")
    dlq_discard.add_argument("entry_id", help="DLQ entry ID to discard")
    dlq_discard.add_argument("--reason", "-r", help="Reason for discarding")
    dlq_discard.set_defaults(func=cmd_dlq_discard)

    # --- config ---
    config_parser = subparsers.add_parser("config", help="Configuration management")
    config_sub = config_parser.add_subparsers(dest="config_command")

    # config validate
    config_validate = config_sub.add_parser("validate", help="Validate a config file")
    config_validate.add_argument("file", help="Config file to validate")
    config_validate.set_defaults(func=cmd_config_validate)

    # config show
    config_show = config_sub.add_parser("show", help="Show effective config")
    config_show.set_defaults(func=cmd_config_show)

    return parser


def cli(argv: Optional[List[str]] = None) -> int:
    """Main CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    if not hasattr(args, "func"):
        # Subcommand group without specific command
        parser.parse_args([args.command, "--help"])
        return 0

    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def main():
    """Script entry point."""
    sys.exit(cli())


if __name__ == "__main__":
    main()
