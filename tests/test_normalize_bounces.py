from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from statistics import median

import pytest

from senderstats.common.address_tools import normalize_bounces


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


@pytest.mark.perf
def test_perf_normalize_bounces(emails):
    f1 = normalize_bounces

    r1 = time_it("test_perf_normalize_bounces", f1, emails, reps=3, rounds=7)

    print(f"\n{r1.name}: {r1.total_ms:,.2f} ms | {r1.ns_per_op:,.1f} ns/op | {r1.ops_per_s:,.0f} ops/s")
