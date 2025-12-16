import os
import time
import pytest

from senderstats.common.address_parser import parse_email_details_tuple

ADDRESS_SAMPLES = [
    "john.doe@example.com",
    "jane_doe@example.co.uk",
    "support@company.com",
    "sales@sub.company.com",
    "admin@localhost",
    "info@my-domain.org",
    "john+news@example.com",
    "alerts+prod@service.io",
    "billing+2025@company.com",
    "John.Doe@Example.COM",
    "first.last.middle@domain.net",
    "user123@example.com",
    "12345@numbers.example",
    "a1b2c3@tracking.service",
    "<20251211.142233.abc123@mail.example.com>",
    "<ABCDEF123456@mail.example.com>",
    "<12345.67890@mx1.company.net>",
    "first-last@my-domain.com",
    "first_last@domain.org",
    "x_y-z@sub.domain.co",
    "user@mail.us-west-2.aws.company.com",
    "noreply@alerts.eu-central-1.service.io",
    "user@[192.168.1.10]",
    "user@[IPv6:2001:db8::1]",
    "\"john doe\"@example.com",
    "\"weird..dots\"@example.org",
    "no-at-symbol.example.com",
    "@missing-local.org",
    "missing-domain@",
    "spaces are bad@example.com",
    "user@.invalid",
    "user@invalid..com",
    "bounce+abc123@mailer.example",
    "campaign-2025_12@marketing.service",
]


def _perf_enabled(request) -> bool:
    return request.config.getoption("--run-perf") or os.environ.get("RUN_PERF", "") == "1"


@pytest.fixture(scope="session")
def addresses():
    n = int(os.environ.get("PERF_COUNT", "200000"))
    return [ADDRESS_SAMPLES[i % len(ADDRESS_SAMPLES)] for i in range(n)]


def _bench_loop(label: str, fn, items, warmup: int = 2000):
    w = min(warmup, len(items))
    for i in range(w):
        fn(items[i])

    t0 = time.perf_counter()
    for s in items:
        fn(s)
    elapsed = time.perf_counter() - t0

    per_sec = len(items) / elapsed if elapsed else float("inf")
    us_per = (elapsed / len(items)) * 1e6 if items else 0.0

    print(
        f"\n{label}: {len(items):,} in {elapsed:.3f}s | "
        f"{per_sec:,.0f}/s | {us_per:.2f} µs/address"
    )
    return elapsed


@pytest.mark.perf
def test_address_parse_tuple_perf(request, addresses):
    if not _perf_enabled(request):
        pytest.skip("perf test skipped (set RUN_PERF=1 or pass --run-perf)")
    _bench_loop("parse_email_details_tuple", parse_email_details_tuple, addresses)


@pytest.mark.perf
def test_address_parse_dataframe_zip_perf(request, addresses):
    if not _perf_enabled(request):
        pytest.skip("perf test skipped (set RUN_PERF=1 or pass --run-perf)")

    import pandas as pd  # import here so non-perf runs don't require pandas

    def transform_zip(df):
        it = df["addr"].fillna("").astype(str)
        dn, ea, dom = zip(*(parse_email_details_tuple(s) for s in it))
        df.loc[:, "display_name"] = dn
        df.loc[:, "email_address"] = ea
        df.loc[:, "domain"] = dom
        return df

    df = pd.DataFrame({"addr": addresses})

    # warmup
    transform_zip(df.iloc[:2000].copy())

    t0 = time.perf_counter()
    out = transform_zip(df.copy())
    elapsed = time.perf_counter() - t0

    rows = len(df)
    per_sec = rows / elapsed if elapsed else float("inf")
    print(f"\nDF zip transform: {rows:,} rows in {elapsed:.3f}s | {per_sec:,.0f} rows/s")

    # sanity
    assert "email_address" in out.columns
