import nox
import socket


COVERAGE_FAIL_LIMIT_PERCENT = 70


@nox.session()
def lint(session):
    session.install("ruff")
    session.run("ruff", "check")


@nox.session()
def format(session):
    session.install("ruff")
    session.run("ruff", "format", "--check")


@nox.session()
def unit(session):
    session.install("-e", ".[testing]")
    session.run(
        "pytest",
        "--cov=dc_etl",
        "--cov=tests.unit",
        "--cov-append",
        "--cov-config",
        ".coveragerc",
        "--cov-report=term-missing",
        f"--cov-fail-under={COVERAGE_FAIL_LIMIT_PERCENT}",
        "tests/unit",
    )


def check_kubo():
    """Check if IPFS (Kubo) is running by attempting to connect to its API port."""
    host = "localhost"
    port = 5001  # Default IPFS API port

    try:
        # Attempt to create a connection to the IPFS API port
        with socket.create_connection((host, port), timeout=5) as _:
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False


@nox.session()
def system(session):
    if not check_kubo():
        session.skip("No IPFS server running")

    session.install("-e", ".[testing]")
    session.run(
        "pytest",
        "--cov=tests.system",
        "--cov-config",
        ".coveragerc",
        "--cov-report=term-missing",
        f"--cov-fail-under={COVERAGE_FAIL_LIMIT_PERCENT}",
        "tests/system",
    )


# Reimport os and shutil for doc test in the future if needed
# import os
# import shutil
# @nox.session()
# def doc(session):
#     session.install("-e", ".[doc]")
#     shutil.rmtree(os.path.join("doc", "_build"), ignore_errors=True)
#     session.run(
#         "sphinx-build",
#         "-W",  # warnings as errors
#         "-T",  # show full traceback on exception
#         os.path.join("doc", ""),
#         os.path.join("doc", "_build"),
#     )
