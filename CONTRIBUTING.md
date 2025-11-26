# Contributing to Job Orchestrator

Thank you for your interest in contributing to Job Orchestrator! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

This project follows a code of conduct that we expect all contributors to adhere to:

- Be respectful and inclusive
- Welcome newcomers and help them learn
- Focus on constructive feedback
- Respect differing viewpoints and experiences

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/job-orchestrator.git
   cd job-orchestrator
   ```
3. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

### Prerequisites

- Python 3.9 or higher
- pip or poetry for dependency management

### Installation

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

3. Install pre-commit hooks (optional but recommended):
   ```bash
   pre-commit install
   ```

## Running Tests

We use pytest for testing. Run tests with:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=job_orchestrator --cov-report=html

# Run specific test file
pytest tests/test_core/test_dag.py

# Run real-world scenario tests
pytest tests/test_real_world/ -v

# Run tests with verbose output
pytest -v

# Run tests in parallel (if pytest-xdist is installed)
pytest -n auto
```

### Test Organization

- `tests/test_core/` - Core functionality tests
- `tests/test_scheduler/` - Scheduler tests
- `tests/test_queue/` - Priority queue tests
- `tests/test_workers/` - Worker pool tests
- `tests/test_locking/` - Distributed locking tests
- `tests/test_integration/` - Integration tests
- `tests/test_real_world/` - Real-world scenario tests (E-commerce, ETL, Microservices)

## Code Style

We follow PEP 8 style guidelines with some modifications:

- **Line length**: 100 characters maximum
- **Imports**: Use `isort` for organizing imports
- **Formatting**: Use `black` for code formatting
- **Type hints**: Use type hints for function signatures
- **Docstrings**: Use Google-style docstrings

### Running Linters

```bash
# Format code
black job_orchestrator tests

# Sort imports
isort job_orchestrator tests

# Type checking
mypy job_orchestrator

# Linting
flake8 job_orchestrator tests
```

## Submitting Changes

1. **Ensure tests pass**:
   ```bash
   pytest
   ```

2. **Commit your changes** with clear commit messages:
   ```bash
   git add .
   git commit -m "Add feature: description of your changes"
   ```

3. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

4. **Create a Pull Request** on GitHub:
   - Provide a clear title and description
   - Reference any related issues
   - Ensure all CI checks pass

### Commit Message Guidelines

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit first line to 72 characters
- Reference issues and pull requests when relevant

Examples:
```
Add retry policy for failed jobs
Fix deadlock in distributed locking
Update documentation for DAG usage
```

## Reporting Issues

When reporting issues, please include:

1. **Clear title** - Brief description of the issue
2. **Description** - Detailed explanation of the problem
3. **Steps to reproduce** - How to trigger the issue
4. **Expected behavior** - What should happen
5. **Actual behavior** - What actually happens
6. **Environment** - Python version, OS, etc.
7. **Code sample** - Minimal code to reproduce (if applicable)

### Issue Template

```markdown
**Description**
A clear description of the issue.

**Steps to Reproduce**
1. Step 1
2. Step 2
3. ...

**Expected Behavior**
What you expected to happen.

**Actual Behavior**
What actually happened.

**Environment**
- Python version: 3.10
- OS: Ubuntu 22.04
- Job Orchestrator version: 0.1.0

**Code Sample**
```python
# Minimal code to reproduce the issue
```
```

## Development Guidelines

### Adding New Features

1. **Discuss first** - Open an issue to discuss major changes
2. **Write tests** - Add tests for new functionality
3. **Update docs** - Update relevant documentation
4. **Follow patterns** - Match existing code style and patterns

### Writing Tests

- Write clear, descriptive test names
- Use fixtures for reusable test setup
- Test edge cases and error conditions
- Aim for high code coverage (>80%)

Example:
```python
def test_job_with_high_priority_executes_first():
    """Test that high priority jobs execute before normal priority jobs."""
    queue = PriorityQueue()
    
    job_normal = Job(id="1", priority=JobPriority.NORMAL)
    job_high = Job(id="2", priority=JobPriority.HIGH)
    
    queue.push(job_normal)
    queue.push(job_high)
    
    first_job = queue.pop()
    assert first_job.id == "2", "High priority job should be first"
```

### Documentation

- Add docstrings to all public functions and classes
- Update README.md for user-facing changes
- Update ARCHITECTURE.md for architectural changes
- Add examples in `examples/` directory for new features
  