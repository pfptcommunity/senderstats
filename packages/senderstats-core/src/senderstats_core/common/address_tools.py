import re
from typing import Iterable, List

_ENTROPY_HEX_PAIRS_RE = re.compile(r'(?=(?:[0-9][a-f]|[a-f][0-9]|[0-9]{2}))', re.IGNORECASE)


def remove_prvs(email: str) -> tuple[str, bool]:
    if not email:
        return email, False

    at = email.find("@")
    if at < 0:
        return email, False

    # fast gate: avoid slicing in the common case
    if not (email.startswith("prvs") or email.startswith("msprvs")):
        return email, False

    local = email[:at]
    first = local.find("=")
    if first < 0:
        return email, False
    second = local.find("=", first + 1)
    if second < 0:
        return email, False

    orig_local = local[second + 1:]
    # Reject empty/garbage originals (e.g. "msprvs=deadbeef==@example.com")
    if not orig_local or orig_local[0] == "=":
        return email, False

    return orig_local + email[at:], True


def remove_prvs_batch(emails: Iterable[str]) -> list[str]:
    out: list[str] = []
    ap = out.append
    for email in emails:
        if not email:
            ap(email)
            continue

        at = email.find("@")
        if at < 0:
            ap(email)
            continue

        if not (email.startswith("prvs") or email.startswith("msprvs")):
            ap(email)
            continue

        local = email[:at]
        first = local.find("=")
        if first < 0:
            ap(email)
            continue
        second = local.find("=", first + 1)
        if second < 0:
            ap(email)
            continue

        orig_local = local[second + 1:]
        # Reject empty/garbage originals (e.g. "msprvs=deadbeef==@example.com")
        if not orig_local or orig_local[0] == "=":
            ap(email)
            continue

        ap(orig_local + email[at:])
    return out


def convert_srs(email: str) -> tuple[str, bool]:
    if not email:
        return email, False

    at = email.find("@")
    if at < 0:
        return email, False

    p = email.find("srs")
    if p < 0 or p >= at:
        return email, False

    # Require "srs" at start OR preceded by '+'
    if p != 0 and email[p - 1] != "+":
        return email, False

    eq0 = email.find("=", p)
    if eq0 < 0 or eq0 >= at:
        return email, False
    eq1 = email.find("=", eq0 + 1)
    if eq1 < 0 or eq1 >= at:
        return email, False
    eq2 = email.find("=", eq1 + 1)
    if eq2 < 0 or eq2 >= at:
        return email, False
    eq3 = email.find("=", eq2 + 1)
    if eq3 < 0 or eq3 >= at:
        return email, False

    orig_domain = email[eq2 + 1: eq3]
    orig_local = email[eq3 + 1: at]
    if not orig_domain or not orig_local:
        return email, False

    return f"{orig_local}@{orig_domain}", True


def convert_srs_batch(emails: Iterable[str]) -> List[str]:
    out: List[str] = []
    ap = out.append

    for email in emails:
        if not email:
            ap(email);
            continue

        at = email.find("@")
        if at < 0:
            ap(email);
            continue

        p = email.find("srs")
        if p < 0 or p >= at:
            ap(email);
            continue

        if p != 0 and email[p - 1] != "+":
            ap(email);
            continue

        eq0 = email.find("=", p)
        if eq0 < 0 or eq0 >= at:
            ap(email);
            continue

        eq1 = email.find("=", eq0 + 1)
        if eq1 < 0 or eq1 >= at:
            ap(email);
            continue

        eq2 = email.find("=", eq1 + 1)
        if eq2 < 0 or eq2 >= at:
            ap(email);
            continue

        eq3 = email.find("=", eq2 + 1)
        if eq3 < 0 or eq3 >= at:
            ap(email);
            continue

        orig_domain = email[eq2 + 1: eq3]
        orig_local = email[eq3 + 1: at]
        if not orig_domain or not orig_local:
            ap(email);
            continue

        ap(orig_local + "@" + orig_domain)

    return out


def normalize_bounces(email: str) -> tuple[str, bool]:
    if not email:
        return email, False

    at = email.find("@")
    if at <= 0:
        return email, False

    if email.startswith("bounces"):
        i = 7
        base = "bounces"
    elif email.startswith("bounce"):
        i = 6
        base = "bounce"
    else:
        return email, False

    if i >= at:
        return email, False

    c = email[i]
    if c != "+" and c != "-":
        return email, False

    return (base + email[at:]), True


def normalize_bounces_batch(emails: Iterable[str]) -> list[str]:
    out: list[str] = []
    ap = out.append

    for email in emails:
        if not email:
            ap(email)
            continue

        at = email.find("@")
        if at <= 0:
            ap(email)
            continue

        # exact prefix gate
        if email.startswith("bounces"):
            i = 7
            base = "bounces"
        elif email.startswith("bounce"):
            i = 6
            base = "bounce"
        else:
            ap(email)
            continue

        if i >= at:
            ap(email)
            continue

        c = email[i]
        if c != "+" and c != "-":
            ap(email)
            continue

        ap(base + email[at:])

    return out


def normalize_entropy(
        email: str,
        entropy_threshold: float = 0.6,
        hex_pair_threshold: int = 6,
) -> tuple[str, bool]:
    try:
        local, domain = email.rsplit("@", 1)
    except ValueError:
        return email, False

    total_length = len(local)
    if total_length == 0:
        return email, False

    numbers = sum(c.isdigit() for c in local)
    symbols = sum(c in "-+=_." for c in local)
    hex_pairs = sum(1 for _ in _ENTROPY_HEX_PAIRS_RE.finditer(local))

    weighted_entropy = (2 * hex_pairs + 1.5 * numbers + 1.5 * symbols) / total_length

    is_entropy = (
            weighted_entropy >= entropy_threshold
            and hex_pairs >= hex_pair_threshold
    )

    if is_entropy:
        return f"#entropy#@{domain}", True

    return email, False
