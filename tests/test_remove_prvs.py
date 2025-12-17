from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from statistics import median

import pytest

from senderstats.common.address_tools import remove_prvs, remove_prvs_parallel


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
        if p < 0.70:
            # normal email
            out.append(f"{user}@{dom}")
        elif p < 0.88:
            # valid prvs form: prvs=hash=original@domain
            h = "".join(rnd.choice("0123456789abcdef") for _ in range(8))
            orig = label()
            prefix = "prvs" if rnd.random() < 0.7 else "msprvs"
            out.append(f"{prefix}={h}={orig}@{dom}")
        elif p < 0.94:
            # malformed: missing second '='
            h = "".join(rnd.choice("0123456789abcdef") for _ in range(8))
            prefix = "prvs" if rnd.random() < 0.7 else "msprvs"
            out.append(f"{prefix}={h}{user}@{dom}")
        elif p < 0.98:
            # malformed: no '@'
            h = "".join(rnd.choice("0123456789abcdef") for _ in range(8))
            prefix = "prvs" if rnd.random() < 0.7 else "msprvs"
            out.append(f"{prefix}={h}={user}")
        else:
            # empty / weird
            out.append("" if rnd.random() < 0.5 else user)

    # a few fixed edge cases
    out.extend([
        "prvs==orig@example.com",  # hash empty but 2 '='
        "msprvs=deadbeef==@example.com",  # original empty
        "prvs=deadbeef=orig@EXAMPLE.COM",  # uppercase domain
        "notprvs=dead=orig@example.com",  # should not trigger
        "@example.com",  # empty local
    ])
    return out


@pytest.fixture(scope="session")
def emails() -> list[str]:
    # tune this to your machine; start at 200k and scale up
    return gen_emails(500_000)


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
    # warm up caches/branch predictors a bit
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

    total_ns = median(samples)
    return Perf(name, total_ns, ops)


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
        "Non-PRVS / Identity",
        {
            "": "",
            "no-at-symbol": "no-at-symbol",
            "user@example.com": "user@example.com",
            "notprvs=dead=orig@example.com": "notprvs=dead=orig@example.com",
            "@example.com": "@example.com",  # empty local, should not change
        },
    ),
    (
        "Malformed PRVS (should not convert)",
        {
            "prvs=deadbeeforig@example.com": "prvs=deadbeeforig@example.com",  # no '=' separators
            "prvs=deadbeef=orig": "prvs=deadbeef=orig",  # no '@'
            "prvs=deadbeeforig": "prvs=deadbeeforig",  # no '@' and no second '='
            "msprvs=deadbeef==@example.com": "msprvs=deadbeef==@example.com",  # empty orig_local -> unchanged
        },
    ),
    (
        "Valid PRVS (should convert)",
        {
            "prvs=deadbeef=orig@example.com": "orig@example.com",
            "msprvs=deadbeef=orig@example.com": "orig@example.com",
            "prvs==orig@example.com": "orig@example.com",  # hash empty but still two '='
            "prvs=deadbeef=orig@EXAMPLE.COM": "orig@EXAMPLE.COM",  # preserves domain case per current logic
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
def test_remove_prvs_cases(suite_name, inp, expected):
    out = remove_prvs(inp)
    assert out == expected, f"[{suite_name}] input={inp!r} out={out!r} expected={expected!r}"


@pytest.mark.perf
def test_remove_prvs(emails):
    l1 = remove_prvs

    r1 = time_it("test_remove_prvs", l1, emails, reps=3, rounds=7)

    print(f"\n{r1.name}: {r1.total_ms:,.2f} ms | {r1.ns_per_op:,.1f} ns/op | {r1.ops_per_s:,.0f} ops/s")


@pytest.mark.perf
def test_perf_remove_prvs_parallel(emails):
    r = time_it_batch(
        "test_perf_remove_prvs_parallel",
        remove_prvs_parallel,
        emails,
        reps=3,
        rounds=7,
        warmup=2,
    )
    print(f"\n{r.name}: {r.total_ms:,.2f} ms | {r.ns_per_op:,.1f} ns/op | {r.ops_per_s:,.0f} ops/s")
