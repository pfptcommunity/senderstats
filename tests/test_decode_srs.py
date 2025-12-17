from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from statistics import median

import pytest

from senderstats.common.address_tools import convert_srs, convert_srs_parallel


def gen_emails(n: int, seed: int = 1337) -> list[str]:
    rnd = random.Random(seed)
    domains = ["example.com", "tld.net", "corp.org", "service.io", "foo.co.uk", "bar.com.au"]

    def label(min_len=3, max_len=12) -> str:
        k = rnd.randint(min_len, max_len)
        s = "".join(rnd.choice(string.ascii_lowercase + string.digits) for _ in range(k))
        return s.strip("-") or "x"

    def token(min_len=3, max_len=10) -> str:
        k = rnd.randint(min_len, max_len)
        return "".join(rnd.choice(string.ascii_letters + string.digits) for _ in range(k))

    out: list[str] = []
    for _ in range(n):
        dom = rnd.choice(domains)
        user = label()

        p = rnd.random()
        if p < 0.75:
            out.append(f"{user}@{dom}")  # normal
        elif p < 0.90:
            # valid-ish SRS according to your logic
            orig_dom = rnd.choice(domains)
            orig_local = label()
            base = "base+" if rnd.random() < 0.5 else ""
            out.append(f"{base}srs0={token()}={token()}={orig_dom}={orig_local}@{dom}")
        elif p < 0.95:
            # malformed: not enough '='
            out.append(f"srs0={token()}={token()}@{dom}")
        else:
            # srs in local but wrong position
            out.append(f"{user}srs0={token()}={token()}={dom}={user}@{dom}")

    out.extend([
        "srs0=AAA=BBB=orig.com=alice@example.net",
        "base+srs0=AAA=BBB=orig.com=alice@example.net",
        "user@srs.example.com",
    ])
    return out


@pytest.fixture(scope="session")
def emails() -> list[str]:
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
        "Non-SRS / Identity",
        {
            "": "",
            "no-at-symbol": "no-at-symbol",
            "user@example.com": "user@example.com",
            "user@srs.example.com": "user@srs.example.com",
        },
    ),
    (
        "Invalid SRS (should not convert)",
        {
            # srs in local but not at start or after '+'
            "xxsrs0=AAA=BBB=orig.com=alice@example.net":
                "xxsrs0=AAA=BBB=orig.com=alice@example.net",

            # not enough '=' tokens
            "srs0@example.net": "srs0@example.net",
            "srs0=AAA=BBB@example.net": "srs0=AAA=BBB@example.net",
            "srs0=AAA=BBB=orig.com@example.net": "srs0=AAA=BBB=orig.com@example.net",

            # empty orig parts
            "srs0=AAA=BBB==alice@example.net":
                "srs0=AAA=BBB==alice@example.net",
            "srs0=AAA=BBB=orig.com=@example.net":
                "srs0=AAA=BBB=orig.com=@example.net",
        },
    ),
    (
        "Valid SRS (should convert)",
        {
            "srs0=AAA=BBB=orig.com=alice@example.net":
                "alice@orig.com",
            "base+srs0=AAA=BBB=orig.com=alice@example.net":
                "alice@orig.com",
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
def test_convert_cases(suite_name, inp, expected):
    out = convert_srs(inp)
    assert out == expected, f"[{suite_name}] input={inp!r} out={out!r}"


@pytest.mark.perf
def test_perf_convert_srs(emails):
    f1 = convert_srs

    r1 = time_it("test_perf_convert_srs", f1, emails, reps=3, rounds=7)

    print(f"\n{r1.name}: {r1.total_ms:,.2f} ms | {r1.ns_per_op:,.1f} ns/op | {r1.ops_per_s:,.0f} ops/s")


@pytest.mark.perf
def test_perf_convert_srs_parallel(emails):
    f1 = convert_srs_parallel
    r = time_it_batch(
        "test_perf_convert_srs_parallel",
        f1,
        emails,
        reps=3,
        rounds=7,
        warmup=2,
    )
    print(f"\n{r.name}: {r.total_ms:,.2f} ms | {r.ns_per_op:,.1f} ns/op | {r.ops_per_s:,.0f} ops/s")
