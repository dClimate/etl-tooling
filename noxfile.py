import os
import pathlib
import shutil

import nox

# numba is not supported on Python 3.12
ALL_INTERPRETERS = (
    "3.10",
    "3.11",
    "3.12",
)
CODE = "dc_etl"
DEFAULT_INTERPRETER = "3.10"
HERE = pathlib.Path(__file__).parent


@nox.session(py=ALL_INTERPRETERS)
def unit(session):
    session.install("-e", ".[testing]")
    session.run(
        "pytest",
        f"--cov={CODE}",
        "--cov=tests.unit",
        "--cov-append",
        "--cov-config",
        HERE / ".coveragerc",
        "--cov-report=term-missing",
        "tests/unit",
    )


@nox.session(py=DEFAULT_INTERPRETER)
def cover(session):
    session.install("coverage")
    session.run("coverage", "report", "--fail-under=100", "--show-missing")
    session.run("coverage", "erase")


@nox.session(py=DEFAULT_INTERPRETER)
def lint(session):
    session.install("black", "flake8", "flake8-pyproject")
    run_black(session, check=True)
    session.run("flake8", CODE, "tests")


@nox.session(py=DEFAULT_INTERPRETER)
def blacken(session):
    # Install all dependencies.
    session.install("black")
    run_black(session)


def run_black(session, check=False):
    args = ["black"]
    if check:
        args.append("--check")
    args.extend(["noxfile.py", CODE, "tests"])
    session.run(*args)


@nox.session(py=DEFAULT_INTERPRETER)
def system(session):
    # if not check_kubo():
    #     session.skip("No IPFS server running")

    session.install("-e", ".[testing]")
    session.run(
        "pytest",
        "--cov=tests.system",
        "--cov-config",
        HERE / ".coveragerc",
        "--cov-report=term-missing",
        "--cov-fail-under=100",
        "tests/system",
    )


# @nox.session(py=DEFAULT_INTERPRETER)
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
