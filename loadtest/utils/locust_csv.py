"""Parse Locust's `--csv` output into `report.py`'s plain stats dict.

A headless run with `--csv=<prefix>` writes `<prefix>_stats.csv`
(request counts + latency percentiles, one row per named request plus
an `Aggregated` row) and `<prefix>_failures.csv` (one row per distinct
error message with an occurrence count -- Locust does not break failures
down by HTTP status code itself, so a 503 count has to be recovered from
the error message text). Both are plain text; no Locust runtime needed
to read them.
"""

from __future__ import annotations

import csv
import io


def parse_aggregated_stats(stats_csv_text: str) -> dict:
    """Extract the `Aggregated` row of a Locust `_stats.csv` file.

    Returns the subset of columns `loadtest/utils/report.py` evaluates
    pass/fail against: `num_requests`, `num_failures`, and the p50/p95/p99
    response-time columns (Locust reports these in milliseconds).
    """
    reader = csv.DictReader(io.StringIO(stats_csv_text))
    for row in reader:
        if row.get("Name") == "Aggregated":
            return {
                "num_requests": int(row["Request Count"]),
                "num_failures": int(row["Failure Count"]),
                "response_time_p50_ms": float(row["50%"]),
                "response_time_p95_ms": float(row["95%"]),
                "response_time_p99_ms": float(row["99%"]),
            }
    raise ValueError("no 'Aggregated' row found in Locust stats CSV")


def count_503_failures(failures_csv_text: str) -> int:
    """Sum `Occurrences` for failure rows whose `Error` mentions 503.

    Locust's failures CSV records each distinct exception/HTTP-error
    message it saw plus how many times, not a status-code column -- this
    is the "503 count" the ticket's Acceptance Criteria and reporting
    section ask each archived run to capture.
    """
    reader = csv.DictReader(io.StringIO(failures_csv_text))
    return sum(
        int(row["Occurrences"]) for row in reader if "503" in row.get("Error", "")
    )
