"""I-023: parsing Locust's `--csv` output into `report.py`'s stats shape
(`loadtest/utils/locust_csv.py`).

Seam choice: Locust writes `<prefix>_stats.csv` (aggregated request
counts/percentiles) and `<prefix>_failures.csv` (one row per distinct
error message with an occurrence count, no status-code column) after
every headless run. Parsing that text into the plain dict
`evaluate_pass_fail`/`build_report` expect is ordinary string/CSV
handling with no Locust runtime or network dependency, so it's unit-
tested directly with real sample output (captured from an actual local
headless run against a throwaway HTTP server while building this suite)
rather than needing Locust itself to run.
"""

from __future__ import annotations

import pytest

from loadtest.utils.locust_csv import count_503_failures, parse_aggregated_stats

STATS_CSV = """\
Type,Name,Request Count,Failure Count,Median Response Time,Average Response Time,Min Response Time,Max Response Time,Average Content Size,Requests/s,Failures/s,50%,66%,75%,80%,90%,95%,98%,99%,99.9%,99.99%,100%
POST,/v1/vote,2416,3,2,1.77,0.78,35.85,2.0,2416.22,0.01,2,2,2,2,2,3,4,6,26,36,36
,Aggregated,2416,3,2,1.77,0.78,35.85,2.0,2416.22,0.01,2,2,2,2,2,3,4,6,26,36,36
"""

FAILURES_CSV = """\
Method,Name,Error,Occurrences,First Seen,Last Seen
POST,/v1/vote,"503 Server Error: Service Unavailable",2,2026-08-29T00:00:00Z,2026-08-29T00:00:01Z
POST,/v1/vote,"409 Client Error: Conflict",1,2026-08-29T00:00:00Z,2026-08-29T00:00:01Z
"""

FAILURES_CSV_NO_503 = """\
Method,Name,Error,Occurrences,First Seen,Last Seen
POST,/v1/vote,"409 Client Error: Conflict",5,2026-08-29T00:00:00Z,2026-08-29T00:00:01Z
"""

EMPTY_FAILURES_CSV = "Method,Name,Error,Occurrences,First Seen,Last Seen\n"


def test_parse_aggregated_stats_reads_the_aggregated_row():
    stats = parse_aggregated_stats(STATS_CSV)
    assert stats["num_requests"] == 2416
    assert stats["num_failures"] == 3
    assert stats["response_time_p50_ms"] == 2
    assert stats["response_time_p95_ms"] == 3
    assert stats["response_time_p99_ms"] == 6


def test_parse_aggregated_stats_raises_when_no_aggregated_row():
    with pytest.raises(ValueError):
        parse_aggregated_stats("Type,Name,Request Count\nPOST,/v1/vote,1\n")


def test_count_503_failures_sums_matching_occurrences():
    assert count_503_failures(FAILURES_CSV) == 2


def test_count_503_failures_is_zero_when_no_503s():
    assert count_503_failures(FAILURES_CSV_NO_503) == 0


def test_count_503_failures_is_zero_for_empty_failures_csv():
    assert count_503_failures(EMPTY_FAILURES_CSV) == 0
