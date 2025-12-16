from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from statistics import median

import pytest

from senderstats.common.tld_parser import get_default_tld_parser


def gen_hosts(n: int, seed: int = 1337) -> list[str]:
    rnd = random.Random(seed)
    suffixes = [
        "com", "net", "org", "edu", "io",
        "co.uk", "org.uk",
        "com.au", "net.au",
        "co.jp",
        "de", "cz", "sk", "fr",
    ]

    def label(min_len=3, max_len=12) -> str:
        k = rnd.randint(min_len, max_len)
        s = "".join(rnd.choice(string.ascii_lowercase + string.digits) for _ in range(k))
        return s.strip("-") or "x"

    out: list[str] = []
    for _ in range(n):
        suf = rnd.choice(suffixes)
        depth = rnd.randint(0, 5)
        parts = [label() for _ in range(depth)]
        reg = label()
        host = ".".join(parts + [reg, suf])

        if rnd.random() < 0.10:
            host = host.upper()

        if rnd.random() < 0.10:
            host = host + "."

        out.append(host)

    out.extend([
        "a.b.c.d.e.f.example.co.uk",
        "EXAMPLE.COM",
        "localhost",
        "192.168.0.1",
        "foo.bar.city.kawasaki.jp",
        "something.appspot.com",
    ])
    return out


@pytest.fixture(scope="session")
def hosts() -> list[str]:
    return gen_hosts(1_000_000)


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


def time_it(name: str, fn, hosts: list[str], *, reps: int = 1, warmup: int = 2000, rounds: int = 7) -> Perf:
    for h in hosts[: min(warmup, len(hosts))]:
        fn(h)

    ops = len(hosts) * reps
    samples: list[int] = []

    for _ in range(rounds):
        t0 = time.perf_counter_ns()
        for _ in range(reps):
            for h in hosts:
                fn(h)
        t1 = time.perf_counter_ns()
        samples.append(t1 - t0)

    total_ns = median(samples)
    return Perf(name, total_ns, ops)


@pytest.mark.perf
def test_perf_split_host(hosts):
    parser = get_default_tld_parser()

    # bind methods once to avoid timing wrapper/caching overhead
    # core1 = parser.split_host_unchecked
    core2 = parser.split_host_safe

    # r1 = time_it("split_host_unchecked", core1, hosts, reps=3, rounds=7)
    r2 = time_it("test_perf_split_host", core2, hosts, reps=3, rounds=7)

    # print(f"\n{r1.name}: {r1.total_ms:,.2f} ms | {r1.ns_per_op:,.1f} ns/op | {r1.ops_per_s:,.0f} ops/s")
    print(f"{r2.name}: {r2.total_ms:,.2f} ms | {r2.ns_per_op:,.1f} ns/op | {r2.ops_per_s:,.0f} ops/s")

    # Assert ensure same output on a sample
    # for h in hosts[:500]:
    #     assert core1(h) == core2(h)
