"""Tests for the CLI module."""

import json
import sys
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from job_orchestrator.cli.main import build_parser, cli, _format_table


class TestFormatTable:
    """Tests for table formatting utility."""

    def test_empty_rows(self):
        result = _format_table(["A", "B"], [])
        assert result == "No results."

    def test_basic_table(self):
        headers = ["ID", "Name", "State"]
        rows = [
            ["abc123", "test_job", "running"],
            ["def456", "other_job", "completed"],
        ]
        result = _format_table(headers, rows)
        lines = result.split("\n")
        assert len(lines) == 4  # header + separator + 2 rows
        assert "ID" in lines[0]
        assert "-" in lines[1]

    def test_column_alignment(self):
        headers = ["Short", "Longer Header"]
        rows = [["a", "b"]]
        result = _format_table(headers, rows)
        assert "Short" in result
        assert "Longer Header" in result


class TestBuildParser:
    """Tests for argument parser construction."""

    def test_parser_creates(self):
        parser = build_parser()
        assert parser is not None
        assert parser.prog == "job-orch"

    def test_status_command(self):
        parser = build_parser()
        args = parser.parse_args(["status"])
        assert args.command == "status"

    def test_version_command(self):
        parser = build_parser()
        args = parser.parse_args(["version"])
        assert args.command == "version"

    def test_jobs_list_command(self):
        parser = build_parser()
        args = parser.parse_args(["jobs", "list", "--state", "running", "--limit", "10"])
        assert args.command == "jobs"
        assert args.jobs_command == "list"
        assert args.state == "running"
        assert args.limit == 10

    def test_jobs_inspect_command(self):
        parser = build_parser()
        args = parser.parse_args(["jobs", "inspect", "abc-123"])
        assert args.job_id == "abc-123"

    def test_jobs_cancel_command(self):
        parser = build_parser()
        args = parser.parse_args(["jobs", "cancel", "abc-123"])
        assert args.job_id == "abc-123"

    def test_dlq_list_command(self):
        parser = build_parser()
        args = parser.parse_args(["dlq", "list", "--status", "pending"])
        assert args.command == "dlq"
        assert args.status == "pending"

    def test_dlq_requeue_command(self):
        parser = build_parser()
        args = parser.parse_args(["dlq", "requeue", "entry-1", "--keep-retries"])
        assert args.entry_id == "entry-1"
        assert args.keep_retries is True

    def test_dlq_discard_command(self):
        parser = build_parser()
        args = parser.parse_args(["dlq", "discard", "entry-1", "--reason", "known bug"])
        assert args.entry_id == "entry-1"
        assert args.reason == "known bug"

    def test_config_validate_command(self):
        parser = build_parser()
        args = parser.parse_args(["config", "validate", "config.yaml"])
        assert args.file == "config.yaml"

    def test_global_json_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--json", "status"])
        assert args.json is True

    def test_global_config_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--config", "/path/to/config.yaml", "status"])
        assert args.config == "/path/to/config.yaml"


class TestCLIEntryPoint:
    """Tests for the cli() function."""

    def test_no_command_shows_help(self, capsys):
        result = cli([])
        assert result == 0

    def test_version_command(self, capsys):
        result = cli(["version"])
        captured = capsys.readouterr()
        assert result == 0
        assert "job-orchestrator" in captured.out

    def test_config_validate_missing_file(self, capsys):
        result = cli(["config", "validate", "nonexistent.yaml"])
        captured = capsys.readouterr()
        assert result == 1
        assert "not found" in captured.err.lower() or "Error" in captured.err
