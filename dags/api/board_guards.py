"""Shared board-health thresholds and consecutive-run latch helpers.

Ashby has no ``meta.total``, so a 200 with 0 jobs is indistinguishable from a
truncated scrape. Greenhouse 404s are usually real dead boards, but a single
failure can be a renamed slug. Both cases wait
``CONSECUTIVE_CONFIRMATION_DAYS`` of the *same* condition before we treat
them as real:

* HTTP 200 + 0 jobs (zero-floor): accept the empty payload so leftover
  active jobs close. The board stays enabled and keeps getting scraped.
* HTTP 404/401/403: flip ``enabled: false`` in YAML and the DB, then close
  leftover jobs.

Transient errors (5xx, network, schema, percentage-drop) reset both
counters so a mixed streak never latches.
"""

CONSECUTIVE_CONFIRMATION_DAYS = 7
MIN_BASELINE_FOR_GUARD = 10


class AshbyGuardTrip(RuntimeError):
    """Raised when an Ashby completeness guard skips the board this run."""

    def __init__(self, message: str, skip_reason: str):
        super().__init__(message)
        self.skip_reason = skip_reason


def should_trip_zero_floor(
    today_count: int,
    baseline: int | None,
    consecutive_zeros: int,
    *,
    min_baseline: int = MIN_BASELINE_FOR_GUARD,
    latch: int = CONSECUTIVE_CONFIRMATION_DAYS,
) -> bool:
    """True when a 0-job Ashby response should be skipped as a bad scrape.

    After ``latch`` consecutive empty HTTP 200s the empty board is treated
    as real and this returns False so the payload can flow through.
    """
    if today_count != 0:
        return False
    if baseline is None or baseline < min_baseline:
        return False
    return consecutive_zeros + 1 < latch


def next_consecutive_counts(zero: int, perm: int, result: dict) -> tuple[int, int]:
    """Return updated ``(consecutive_zero_scrapes, consecutive_permanent_failures)``.

    ``result`` is a ``scrape_all_companies`` summary (success or sentinel).
    """
    skipped = bool(result.get("skipped"))
    error_type = result.get("error_type")
    skip_reason = result.get("skip_reason")
    total_jobs = int(result.get("total_jobs") or 0)

    if not skipped:
        if total_jobs == 0:
            return zero + 1, 0
        return 0, 0
    if error_type == "permanent" or skip_reason == "permanent_http":
        return 0, perm + 1
    if skip_reason == "zero_floor":
        return zero + 1, 0
    return 0, 0


def companies_to_force_close(
    company_results: list[dict] | None,
    board_outcomes: dict | None,
) -> list[str]:
    """Company names whose leftover active jobs should close this run.

    Includes boards that successfully scraped 0 jobs (confirmed empty) and
    boards that ``disable_dead_boards`` just flipped off after the 404 latch.
    """
    names: set[str] = set()
    for result in company_results or []:
        if not isinstance(result, dict) or result.get("skipped"):
            continue
        if int(result.get("total_jobs") or 0) != 0:
            continue
        name = result.get("company_name")
        if name:
            names.add(name)
    for name in (board_outcomes or {}).get("disabled") or []:
        names.add(name)
    return sorted(names)
