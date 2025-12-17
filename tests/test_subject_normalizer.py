import time

import pytest

from senderstats.common.subject_normalizer import normalize_subject

iso_tests = {
    # originals
    "2025-12-11": "{d}",
    "2025-12-11 14:22": "{t}",
    "2025-12-11T14:22": "{t}",
    "2025-12-11T14:22:33": "{t}",

    # extra ISO / YMD variants
    "0001-01-01": "{d}",
    "9999-12-31": "{d}",
    "2025/12/11": "{d}",
    "2025/12/11 23:59": "{t}",
    "2025.12.11 23:59:59": "{t}",
}

numeric_date_tests = {
    # originals
    "12/11/2025": "{d}",
    "12-11-25": "{d}",
    "1/2/25": "{d}",
    "1/2/2025 14:00": "{t}",

    # more numeric variants
    "10.2.25": "{d}",
    "01.12.0000": "{d}",
    "12/11/0001": "{d}",
    "3-1-99": "{d}",
    "03-01-1999 08:15": "{t}",
    "10.12.2025 08:15 - 09:00": "{t}",
}

month_day_year_tests = {
    # originals
    "Dec 11, 2025": "{d}",
    "Dec 11 2025": "{d}",
    "Dec 11": "{d}",
    "Dec, 11": "{d}",
    "Dec, 11, 2025": "{d}",
    "December 11": "{d}",
    "December 11 2025": "{d}",

    # extras
    "Dec 11 0000": "{d}",
    "December 31 9999": "{d}",
    "Dec  11,   2025": "{d}",
    "December 5, 25": "{d}",
    "Dec 11 2025 23:59": "{t}",
}

day_month_year_tests = {
    # originals
    "11 Dec 2025": "{d}",
    "11 Dec, 2025": "{d}",
    "11th Dec 2025": "{d}",
    "11th Dec, 2025": "{d}",

    # extras
    "01 Jan 0000": "{d}",
    "31 Dec 9999": "{d}",
    "1st Jan 25": "{d}",
    "1st Jan 25 14:00": "{t}",
    "11 Dec 2025 14:00 UTC": "{t}",
    "11 Dec 2025 14:00:59 PST": "{t}",
}

dow_date_tests = {
    # originals
    "Thu Dec 11, 2025": "{d}",
    "Thu, Dec 11, 2025": "{d}",
    "Mon Dec 1 2025": "{d}",
    "Tuesday, December 2 2025": "{d}",

    # extras
    "Wed 1/2/25": "{d}",
    "Fri 2025-12-11": "{d}",
    "fri 2025-12-11 14:00": "{t}",
    "Tuesday, 11 Dec 2025 14:00 - 15:30": "{t}",
}

ampm_tests = {
    # originals
    "Dec 11, 2025 2:30pm": "{t}",
    "Dec 11 2025 02:30 PM": "{t}",
    "11 Dec 2025 2:30pm": "{t}",
    "Thu Dec 11, 2025 2:30pm": "{t}",

    # extras
    "Dec 11, 2025 2pm": "{t}",
    "Dec 11, 2025 2 pm": "{t}",
    "11 Dec 2025 2 PM": "{t}",
    "11 Dec 2025 2:30 pm PST": "{t}",
}

time_range_tests = {
    # originals
    "Dec 11, 2025 2:30pm - 3:15pm": "{t}",
    "Thu Dec 11, 2025 2:45pm - 3:15pm (EST)": "{t}",
    "11 Dec 2025 14:00 - 15:30": "{t}",

    # extras
    "Dec 11 2025 2pm-3pm": "{t}",
    "11 Dec 2025 14:00 - 15:30 UTC": "{t}",
    "2025-12-11 14:00 - 15:30 (PST)": "{t}",
}

month_only_tests = {
    # originals
    "Dec": "{m}",
    "December": "{m}",
    "jul": "{m}",  # lower case, should still match
    "Meeting in October": "meeting in {m}",

    # extras
    "Incentive ends in July": "incentive ends in {m}",
    "Don't forget about your appointment, in jan": "don't forget about your appointment, in {m}",
    "Billed through NOVEMBER": "billed through {m}",
}

id_tests = {
    # originals
    "Order #hsgske-heys": "order {#}",
    "Tracking ABC123": "tracking {#}",
    "Product A-1234 shipped Dec 11, 2025": "product {#} shipped {d}",

    # extras
    "Ref: INV-2025-12-11": "ref: {#}",
    "Ticket ID XZ-99-2025 opened on 11 Dec 2025":
        "ticket id {#} opened on {d}",
}

int_tests = {
    # originals
    "Invoice 12345": "invoice {i}",
    "Your code is 987": "your code is {i}",
    "Apartment 403": "apartment {i}",

    # extras
    "Balance: 0": "balance: {i}",
    "10 messages in your mailbox": "{i} messages in your mailbox",
}

realistic_tests = {
    # originals
    "Appt confirmed: Thu Dec 11, 2025 2:45pm - 3:15pm (EST)":
        "appt confirmed: {t}",
    "Your appointment is scheduled for 04:30pm Mon, Dec 1, 2025":
        "your appointment is scheduled for {t}",
    "Your package expected Dec, 24":
        "your package expected {d}",
    "Parcel #abc-999 will arrive on December 5 2025":
        "parcel {#} will arrive on {d}",
    "Invoice 123 for order #hsgske-heys on 2025-12-03":
        "invoice {i} for order {#} on {d}",

    # extras
    "Order 123 placed on Dec 11, 2025 at 2:30pm":
        "order {i} placed on {d} at {tm}",
    "Reminder: Fri 1/2/25 9:00am - 10:00am (PST)":
        "reminder: {t}",
    "Invoice statement for December 11 2025":
        "invoice statement for {d}",
    "Your subscription renews in December":
        "your subscription renews in {m}",
    "Your code 987 expires on 2025-12-11":
        "your code {i} expires on {d}",
}

time_only_tests = {
    "3pm": "{tm}",
    "3 pm": "{tm}",
    "03:15": "{tm}",
    "03:15 PM": "{tm}",
    "14:00": "{tm}",
    "14:00 UTC": "{tm}",
    "Meeting at 3pm": "meeting at {tm}",
    "Meeting at 3 pm": "meeting at {tm}",
}

# --- Pytest glue ---

TEST_SUITES = [
    ("ISO Tests", iso_tests),
    ("Numeric Dates", numeric_date_tests),
    ("Month Day Year", month_day_year_tests),
    ("Day Month Year", day_month_year_tests),
    ("DOW + Date", dow_date_tests),
    ("AM/PM Tests", ampm_tests),
    ("Time Ranges", time_range_tests),
    ("Month Only", month_only_tests),
    ("Time Only", time_only_tests),
    ("ID Tests", id_tests),
    ("Integer Tests", int_tests),
    ("Realistic Mixed", realistic_tests),
]


def _flatten_suites():
    """Yield (suite_name, input_text, expected_output) for parametrization."""
    for suite_name, cases in TEST_SUITES:
        for inp, expected in cases.items():
            yield suite_name, inp, expected


@pytest.mark.parametrize(
    "suite_name,inp,expected",
    list(_flatten_suites()),
    ids=lambda v: v if isinstance(v, str) else repr(v),
)
def test_subject_normalizer_cases(suite_name, inp, expected):
    out = normalize_subject(inp)
    assert out == expected, f"[{suite_name}] input={inp!r} out={out!r}"


@pytest.mark.perf
def test_perf_subject_normalizer():
    # NOTE: avoid building a huge list; loop over a constant sample
    sample = "Linda - Invoice 123 for order #hsgske-heys on 2025-12-03"

    n = 1_000_000

    # warmup
    for _ in range(20_000):
        normalize_subject(sample)

    t0 = time.perf_counter()
    for _ in range(n):
        normalize_subject(sample)
    elapsed = time.perf_counter() - t0

    per_sec = n / elapsed if elapsed else float("inf")
    us_per = (elapsed / n) * 1e6 if n else 0.0

    print(f"\nnormalize_subject: {n:,} in {elapsed:.3f}s | {per_sec:,.0f}/s | {us_per:.2f} µs/subject")
