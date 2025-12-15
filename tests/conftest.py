import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--run-perf",
        action="store_true",
        default=False,
        help="Run performance tests (skipped by default).",
    )

def pytest_configure(config):
    config.addinivalue_line("markers", "perf: performance tests (skipped by default)")
