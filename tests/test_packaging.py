"""Verify the installable wheel, not just imports from the checkout."""

import subprocess
import sys
import zipfile
from pathlib import Path
from shutil import copy2, copytree


def test_wheel_contains_only_job_orchestrator(tmp_path):
    root = Path(__file__).resolve().parents[1]
    project = tmp_path / "project"
    project.mkdir()
    for name in ("pyproject.toml", "README.md", "LICENSE"):
        copy2(root / name, project / name)
    copytree(root / "src", project / "src", ignore=lambda _, names: [
        name for name in names if name == "__pycache__" or name.endswith(".egg-info")
    ])
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation"],
        cwd=project, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    wheel, = (project / "dist").glob("*.whl")
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        unexpected = [
            name for name in names
            if not name.startswith("job_orchestrator/")
            and ".dist-info/" not in name
        ]
        assert unexpected == [], f"Unrelated modules shipped: {unexpected}"
        assert "job_orchestrator/py.typed" in names
        assert "job_orchestrator/cli/main.py" in names
        entry_points = next(name for name in names if name.endswith("/entry_points.txt"))
        assert b"job-orch = job_orchestrator.cli.main:main" in archive.read(entry_points)
