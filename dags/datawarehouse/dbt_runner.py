"""
Run the project's dbt from Airflow tasks.

dbt lives in its OWN virtualenv (baked into the image by the Dockerfile) so its
dependency tree can never clash with Airflow's pinned packages. Rather than
import dbt into the Airflow process, we shell out to that venv's `dbt` binary
and read the machine-readable ``target/run_results.json`` it leaves behind.

Two entry points are used by the DAG:
  * ``build_marts()``      -> runs `dbt run` (Step 7). Raises on failure so the
                             task (and the DAG) fails loudly if the analytics
                             build breaks.
  * ``collect_test_warnings()`` -> runs `dbt test` (Step 9, warn-only). NEVER
                             raises; returns a list of human-readable warning
                             lines that the QC task folds into its single
                             Telegram summary.

Everything is overridable via env vars so the same code runs locally and in the
container:
  DBT_BIN          path to the dbt executable  (default: image venv)
  DBT_PROJECT_DIR  dbt project + profiles dir  (default: /opt/airflow/dbt)
  DBT_TARGET       profiles.yml target         (default: prod -> public schema)
"""

import json
import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

DBT_BIN = os.environ.get("DBT_BIN", "/opt/airflow/dbt_venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")

# The project dir is mounted read-only, so send dbt's writable artifacts
# (target/, logs/) to a scratch location. run_results.json lands under target/.
_TARGET_PATH = os.environ.get("DBT_TARGET_PATH", "/tmp/dbt_target")
_LOG_PATH = os.environ.get("DBT_LOG_PATH", "/tmp/dbt_logs")


def _dbt_env() -> dict:
    env = os.environ.copy()
    env["DBT_TARGET_PATH"] = _TARGET_PATH
    env["DBT_LOG_PATH"] = _LOG_PATH
    return env


def _run(command: str, *, select: str | None = None) -> subprocess.CompletedProcess:
    """Invoke `dbt <command>` for this project. Streams dbt's output to the log."""
    cmd = [
        DBT_BIN, command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
        "--target", DBT_TARGET,
    ]
    if select:
        cmd += ["--select", select]

    logger.info("Running: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd, cwd=DBT_PROJECT_DIR, env=_dbt_env(),
        capture_output=True, text=True,
    )
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    return proc


def _parse_run_results() -> list[dict]:
    """Read the results of the most recent dbt invocation, or [] if absent."""
    path = Path(_TARGET_PATH) / "run_results.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text()).get("results", [])
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not read dbt run_results.json: %s", e)
        return []


def _friendly(unique_id: str) -> str:
    """`test.jobpulse.jobs_salary_plausible` -> `jobs_salary_plausible`."""
    return unique_id.split(".")[2] if unique_id.count(".") >= 2 else unique_id


def build_marts() -> dict:
    """`dbt run`: rebuild company_stats / company_skills / company_departments.

    Raises RuntimeError on any dbt failure so the Airflow task fails (and the
    on_failure_callback fires) — analytics silently not building would be worse
    than a loud failure.
    """
    proc = _run("run")
    results = _parse_run_results()
    built = [_friendly(r["unique_id"]) for r in results if r.get("status") == "success"]

    if proc.returncode != 0:
        raise RuntimeError(
            f"dbt run failed (exit {proc.returncode}); built {built}. See logs above."
        )
    logger.info("dbt run built %d model(s): %s", len(built), built)
    return {"models_built": len(built), "models": built}


def collect_test_warnings() -> list[str]:
    """`dbt test`: return warning lines for the QC summary. Never raises.

    All project tests default to `warn` severity, so a normal run exits 0 with
    zero or more WARN nodes. We surface WARN (data findings), plus FAIL/ERROR
    (a real test or connection problem) as warnings too — QC must never crash
    the DAG, so even a dbt invocation failure becomes a single warning line.
    """
    try:
        proc = _run("test")
        results = _parse_run_results()
    except Exception as e:  # noqa: BLE001 — warn-only, never crash the DAG
        logger.warning("dbt test invocation failed: %s", e)
        return [f"dbt test could not run: {e}"]

    warnings: list[str] = []
    for r in results:
        status = r.get("status")
        name = _friendly(r.get("unique_id", ""))
        failures = r.get("failures")
        if status == "warn":
            warnings.append(f"dbt {name}: {failures} row(s)")
        elif status in ("fail", "error"):
            detail = r.get("message") or f"{failures} row(s)"
            warnings.append(f"dbt {name}: {status.upper()} ({detail})")

    # A non-zero exit with no parsed WARN/FAIL rows means dbt itself errored
    # (bad SQL, no DB, etc.) — surface that rather than swallow it.
    if proc.returncode != 0 and not warnings:
        warnings.append(f"dbt test exited {proc.returncode}; see task logs.")

    logger.info("dbt test produced %d warning(s)", len(warnings))
    return warnings
