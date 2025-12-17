from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from statistics import median

import pytest

from senderstats.common.address_tools import normalize_bounces, normalize_bounces_parallel


def gen_emails(n: int, seed: int = 1337) -> list[str]:
    rnd = random.Random(seed)
    domains = ["example.com", "tld.net", "corp.org", "service.io", "foo.co.uk", "bar.com.au"]

    def label(min_len=3, max_len=12) -> str:
        k = rnd.randint(min_len, max_len)
        s = "".join(rnd.choice(string.ascii_lowercase + string.digits) for _ in range(k))
        return s.strip("-") or "x"

    out: list[str] = []
    for _ in range(n):
        dom = rnd.choice(domains)
        user = label()

        p = rnd.random()
        if p < 0.85:
            # normal email (most common)
            out.append(f"{user}@{dom}")
        elif p < 0.94:
            # bounce / bounces with + or - (valid)
            base = "bounces" if rnd.random() < 0.5 else "bounce"
            sep = "+" if rnd.random() < 0.5 else "-"
            tag = label(5, 20)
            out.append(f"{base}{sep}{tag}@{dom}")
        elif p < 0.97:
            # starts with bounce but wrong delimiter (invalid)
            base = "bounces" if rnd.random() < 0.5 else "bounce"
            out.append(f"{base}{label(1, 3)}@{dom}")  # next char not +/-
        else:
            # malformed: missing @ or empty local
            out.append("" if rnd.random() < 0.5 else f"@{dom}")

    # fixed edge cases
    out.extend([
        "bounce+tag@example.com",
        "bounces-tag@example.com",
        "bounce@example.com",  # no +/-
        "bounces@example.com",  # no +/-
        "bounce+@example.com",  # delimiter present, empty tag still ok per logic
        "@example.com",  # empty local
        "noatsymbol",
        "",
    ])
    return out


@pytest.fixture(scope="session")
def emails() -> list[str]:
    # tune size as needed
    return gen_emails(1_000_000)


@dataclass(frozen=True)
class Perf:
    name: str
    total_ns: int
    ops: int

    @property
    def ns_per_op(self) -> float:
        return self.total_ns / self.ops

    @property
    def ops_per_s(self) -> float:
        return 1e9 / self.ns_per_op

    @property
    def total_ms(self) -> float:
        return self.total_ns / 1e6


def time_it(name: str, fn, items: list[str], *, reps: int = 1, warmup: int = 2000, rounds: int = 7) -> Perf:
    for x in items[: min(warmup, len(items))]:
        fn(x)

    ops = len(items) * reps
    samples: list[int] = []

    for _ in range(rounds):
        t0 = time.perf_counter_ns()
        for _ in range(reps):
            for x in items:
                fn(x)
        t1 = time.perf_counter_ns()
        samples.append(t1 - t0)

    return Perf(name, median(samples), ops)


def time_it_batch(
        name: str,
        fn,
        items: list[str],
        *,
        reps: int = 1,
        rounds: int = 7,
        warmup: int = 1,
) -> Perf:
    for _ in range(warmup):
        out = fn(items)
        _ = len(out)

    ops = len(items) * reps
    samples: list[int] = []
    sink = 0

    for _ in range(rounds):
        t0 = time.perf_counter_ns()
        for _ in range(reps):
            out = fn(items)
        t1 = time.perf_counter_ns()

        if out:
            sink ^= (len(out[0]) if out[0] else 0)
            sink ^= (len(out[-1]) if out[-1] else 0)
            sink ^= len(out)

        samples.append(t1 - t0)

    total_ns = median(samples)
    if sink == -1:
        raise AssertionError("sink")
    return Perf(name, total_ns, ops)


TEST_SUITES = [
    (
        "Non-bounce / Identity",
        {
            "": "",
            "noatsymbol": "noatsymbol",
            "@example.com": "@example.com",  # empty local
            "user@example.com": "user@example.com",  # normal
            "rebounce+tag@example.com": "rebounce+tag@example.com",  # prefix not exact
            "bounc@example.com": "bounc@example.com",  # not exact prefix
        },
    ),
    (
        "Valid bounce tags (should normalize)",
        {
            "bounce+tag@example.com": "bounce@example.com",
            "bounce-tag@example.com": "bounce@example.com",
            "bounces+tag@example.com": "bounces@example.com",
            "bounces-tag@example.com": "bounces@example.com",
            "bounce+@example.com": "bounce@example.com",  # empty tag still normalizes
            "bounces-@example.com": "bounces@example.com",  # empty tag still normalizes
        },
    ),
    (
        "Invalid bounce tags (should not normalize)",
        {
            "bounce@example.com": "bounce@example.com",  # no +/- after prefix
            "bounces@example.com": "bounces@example.com",  # no +/- after prefix
            "bounceXtag@example.com": "bounceXtag@example.com",  # not prefix match
            "bounce_tag@example.com": "bounce_tag@example.com",  # wrong delimiter
            "bounces.tag@example.com": "bounces.tag@example.com",  # wrong delimiter
            "bounce+tag": "bounce+tag",  # no '@'
            "bounces-tag": "bounces-tag",  # no '@'
        },
    ),
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
def test_normalize_bounces_cases(suite_name, inp, expected):
    out = normalize_bounces(inp)
    assert out == expected, f"[{suite_name}] input={inp!r} out={out!r} expected={expected!r}"


@pytest.mark.perf
def test_perf_normalize_bounces(emails):
    f1 = normalize_bounces

    r1 = time_it("test_perf_normalize_bounces", f1, emails, reps=3, rounds=7)

    print(f"\n{r1.name}: {r1.total_ms:,.2f} ms | {r1.ns_per_op:,.1f} ns/op | {r1.ops_per_s:,.0f} ops/s")


@pytest.mark.perf
def test_perf_normalize_bounces_parallel(emails):
    r = time_it_batch(
        "test_perf_normalize_bounces_parallel",
        normalize_bounces_parallel,
        emails,
        reps=3,
        rounds=7,
        warmup=2,
    )
    print(f"\n{r.name}: {r.total_ms:,.2f} ms | {r.ns_per_op:,.1f} ns/op | {r.ops_per_s:,.0f} ops/s")
