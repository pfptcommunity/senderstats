import os
import time
import pytest

from senderstats.common.subject_normalizer import get_default_normalizer


@pytest.mark.perf
def test_subject_normalizer_perf(request):
    if not request.config.getoption("--run-perf") and os.environ.get("RUN_PERF", "") != "1":
        pytest.skip("perf test skipped (set RUN_PERF=1 or pass --run-perf)")

    snorm = get_default_normalizer()

    subjects = ["Linda - Invoice 123 for order #hsgske-heys on 2025-12-03"] * 200_000

    # warmup
    for s in subjects[:2000]:
        snorm.normalize(s)

    t0 = time.perf_counter()
    for s in subjects:
        snorm.normalize(s)
    elapsed = time.perf_counter() - t0

    per_sec = len(subjects) / elapsed if elapsed else float("inf")
    us_per = (elapsed / len(subjects)) * 1e6 if subjects else 0.0

    print(f"\nNormalized {len(subjects):,} in {elapsed:.3f}s | {per_sec:,.0f}/s | {us_per:.2f} µs/subject")
