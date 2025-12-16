from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from statistics import median

import pytest

from senderstats.common.mid_parser import MIDParser
from senderstats.common.tld_parser import TLDParser


def gen_message_ids(n: int, seed: int = 1337) -> list[str]:
    rnd = random.Random(seed)

    domains = [
        "example.com", "tld.net", "corp.org", "service.io",
        "foo.co.uk", "bar.com.au", "localhost", "mail.corp",
    ]

    def atom(min_len=3, max_len=16) -> str:
        k = rnd.randint(min_len, max_len)
        chars = string.ascii_letters + string.digits
        return "".join(rnd.choice(chars) for _ in range(k))

    def host() -> str:
        # occasionally include multi-label hosts and uppercase
        labels = [atom(2, 10).lower() for _ in range(rnd.randint(1, 4))]
        h = ".".join(labels + [rnd.choice(domains)])
        return h.upper() if rnd.random() < 0.10 else h

    out: list[str] = []
    for _ in range(n):
        p = rnd.random()

        if p < 0.70:
            # Typical RFC-ish Message-ID: <left@right>
            left = atom(6, 24)
            right = host()
            out.append(f"<{left}@{right}>")

        elif p < 0.82:
            # No angle brackets
            left = atom(6, 24)
            right = host()
            out.append(f"{left}@{right}")

        elif p < 0.90:
            # Extra dots / mixed tokens
            left = f"{atom(3,8)}.{atom(3,8)}.{atom(3,8)}"
            right = host()
            out.append(f"<{left}@{right}>")

        elif p < 0.94:
            # Missing '@' (invalid/noncompliant)
            out.append(f"<{atom(10,30)}>")

        elif p < 0.97:
            # Missing right side
            out.append(f"<{atom(10,30)}@>")

        elif p < 0.99:
            # Missing left side
            out.append(f"<@{host()}>")

        else:
            # Weird/empty/whitespace
            out.append("" if rnd.random() < 0.5 else "   <notreally>   ")

    # fixed edge cases / common oddities
    out.extend([
        "<abc@EXAMPLE.COM>",
        "abc@EXAMPLE.COM",
        "<abc.def@foo.bar.city.kawasaki.jp>",
        "<abc@192.168.0.1>",          # not RFC domain, but seen in logs
        "<abc@[192.168.0.1]>",        # bracket-literal style
        "<abc@localhost>",
        "<@example.com>",
        "<abc@>",
        "<abc>",
        "noatsymbol",
        "",
    ])
    return out


@pytest.fixture(scope="session")
def msgids() -> list[str]:
    return gen_message_ids(1_000_000)


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
def test_perf_message_id_parser_old_vs_new(msgids):
    mid_parser = MIDParser(TLDParser.load_default())

    new = mid_parser.parse

    r1 = time_it("parse_message_id_old", new, msgids, reps=3, rounds=7)

    print(f"\n{r1.name}: {r1.total_ms:,.2f} ms | {r1.ns_per_op:,.1f} ns/op | {r1.ops_per_s:,.0f} ops/s")

