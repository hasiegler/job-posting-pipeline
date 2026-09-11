"""Unit tests for the 7-day empty-board / 404 confirmation latch.

Pure stdlib — board_guards.py has no third-party imports, so CI can run
these without Postgres, requests, or Airflow.
"""

from __future__ import annotations

import os
import sys
import unittest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DAGS_DIR = os.path.join(REPO_ROOT, "dags")
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

from api.board_guards import (  # noqa: E402
    CONSECUTIVE_CONFIRMATION_DAYS,
    MIN_BASELINE_FOR_GUARD,
    companies_to_force_close,
    next_consecutive_counts,
    should_trip_zero_floor,
)


class ShouldTripZeroFloorTests(unittest.TestCase):
    def test_nonzero_count_never_trips(self):
        self.assertFalse(should_trip_zero_floor(15, 15, 0))

    def test_small_baseline_never_trips(self):
        self.assertFalse(should_trip_zero_floor(0, MIN_BASELINE_FOR_GUARD - 1, 0))
        self.assertFalse(should_trip_zero_floor(0, None, 0))

    def test_first_six_zeros_trip(self):
        for consec in range(CONSECUTIVE_CONFIRMATION_DAYS - 1):
            self.assertTrue(
                should_trip_zero_floor(0, 15, consec),
                msg=f"day {consec + 1} should still skip",
            )

    def test_seventh_zero_is_accepted(self):
        self.assertFalse(
            should_trip_zero_floor(0, 15, CONSECUTIVE_CONFIRMATION_DAYS - 1)
        )

    def test_subsequent_zeros_stay_accepted(self):
        self.assertFalse(
            should_trip_zero_floor(0, 15, CONSECUTIVE_CONFIRMATION_DAYS)
        )
        self.assertFalse(should_trip_zero_floor(0, 15, 99))


class NextConsecutiveCountsTests(unittest.TestCase):
    def test_success_with_jobs_resets_both(self):
        self.assertEqual(
            next_consecutive_counts(6, 3, {"skipped": False, "total_jobs": 12}),
            (0, 0),
        )

    def test_success_with_zero_jobs_bumps_zero_resets_perm(self):
        self.assertEqual(
            next_consecutive_counts(6, 3, {"skipped": False, "total_jobs": 0}),
            (7, 0),
        )

    def test_zero_floor_skip_bumps_zero_resets_perm(self):
        self.assertEqual(
            next_consecutive_counts(
                2, 4,
                {"skipped": True, "error_type": "transient", "skip_reason": "zero_floor"},
            ),
            (3, 0),
        )

    def test_permanent_http_bumps_perm_resets_zero(self):
        self.assertEqual(
            next_consecutive_counts(
                5, 6,
                {
                    "skipped": True,
                    "error_type": "permanent",
                    "skip_reason": "permanent_http",
                    "total_jobs": 0,
                },
            ),
            (0, 7),
        )

    def test_transient_resets_both(self):
        self.assertEqual(
            next_consecutive_counts(
                5, 6,
                {
                    "skipped": True,
                    "error_type": "transient",
                    "skip_reason": "percentage_drop",
                    "total_jobs": 0,
                },
            ),
            (0, 0),
        )

    def test_network_error_resets_both(self):
        self.assertEqual(
            next_consecutive_counts(
                4, 2,
                {"skipped": True, "error_type": "transient", "skip_reason": "network"},
            ),
            (0, 0),
        )


class CompaniesToForceCloseTests(unittest.TestCase):
    def test_successful_empty_scrape(self):
        self.assertEqual(
            companies_to_force_close(
                [{"company_name": "deel_ashby", "skipped": False, "total_jobs": 0}],
                {"disabled": []},
            ),
            ["deel_ashby"],
        )

    def test_skipped_empty_is_not_closed(self):
        self.assertEqual(
            companies_to_force_close(
                [{
                    "company_name": "deel_ashby",
                    "skipped": True,
                    "total_jobs": 0,
                    "skip_reason": "zero_floor",
                }],
                None,
            ),
            [],
        )

    def test_disabled_boards_are_closed(self):
        self.assertEqual(
            companies_to_force_close(
                [{"company_name": "clickhouse_greenhouse", "skipped": True, "total_jobs": 0}],
                {"disabled": ["clickhouse_greenhouse"]},
            ),
            ["clickhouse_greenhouse"],
        )

    def test_successful_nonzero_not_closed(self):
        self.assertEqual(
            companies_to_force_close(
                [{"company_name": "notion_ashby", "skipped": False, "total_jobs": 40}],
                {"disabled": []},
            ),
            [],
        )

    def test_union_and_sort(self):
        self.assertEqual(
            companies_to_force_close(
                [
                    {"company_name": "zeta", "skipped": False, "total_jobs": 0},
                    {"company_name": "alpha", "skipped": False, "total_jobs": 0},
                ],
                {"disabled": ["mid"]},
            ),
            ["alpha", "mid", "zeta"],
        )


if __name__ == "__main__":
    unittest.main()
