from typing import Dict, Tuple, Iterable, List

import regex as re

_EMAIL_PATTERN = (
    r"^[A-Za-z0-9!#$%&'*+/=?^_`{|}~.-]+@"
    r"(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*)$"
)

_EMAIL_RE = re.compile(_EMAIL_PATTERN)

_EMAIL_STRIP_CHARS = " \t\r\n,;<>\"'()"


def _find_angle_pair_outside_quotes(s: str) -> Tuple[int, int]:
    in_quotes = False
    escape = False
    lt = -1
    gt = -1

    for i, ch in enumerate(s):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_quotes = not in_quotes
            continue
        if not in_quotes:
            if ch == "<":
                lt = i
                gt = -1
            elif ch == ">" and lt != -1:
                gt = i

    return lt, gt


def _unescape_quoted_display(name: str) -> str:
    out = []
    i = 0
    n = len(name)
    while i < n:
        if name[i] == "\\" and i + 1 < n and name[i + 1] in ('\\', '"'):
            out.append(name[i + 1])
            i += 2
        else:
            out.append(name[i])
            i += 1
    return "".join(out)


def _fallback_parse_last_at_token(s: str) -> Tuple[str, str]:
    at = s.rfind("@")
    if at == -1:
        return s, ""
    j = at - 1
    while j >= 0 and not s[j].isspace():
        j -= 1
    if j >= 0:
        return s[:j].strip(), s[j + 1:].strip()
    return "", s


def parse_email_details(email_str: str) -> Dict[str, str]:
    original = email_str
    s = (email_str or "").strip()

    data = {"display_name": "", "email_address": "", "odata": original}
    if not s:
        return data

    display = ""
    email = ""

    lt, gt = _find_angle_pair_outside_quotes(s)
    if lt != -1 and gt != -1 and lt < gt:
        disp1 = s[:lt].strip()
        em1 = s[lt + 1:gt].strip().strip(_EMAIL_STRIP_CHARS)
        if _EMAIL_RE.fullmatch(em1):
            display, email = disp1, em1
        else:
            display, email = _fallback_parse_last_at_token(s)
    else:
        display, email = _fallback_parse_last_at_token(s)

    email = (email or "").strip().strip(_EMAIL_STRIP_CHARS)
    if email and not _EMAIL_RE.fullmatch(email):
        email = ""

    display = (display or "").strip().rstrip(",").strip()
    if len(display) >= 2 and display[0] == '"' and display[-1] == '"':
        display = _unescape_quoted_display(display[1:-1]).strip()

    data["display_name"] = display
    data["email_address"] = email
    return data


def parse_email_details_tuple(s: str) -> Tuple[str, str]:
    d = parse_email_details(s)  # your fast function
    return d["display_name"], d["email_address"]


def parse_email_details_parallel(
        emails: Iterable[str],
) -> Tuple[List[str], List[str]]:
    """
    Batch parse for speed. Returns 2 parallel lists:
      display_name[], email_address[]
    """
    display_names: List[str] = []
    email_addresses: List[str] = []

    # local bindings reduce attribute/global lookups in hot loops
    parse_one = parse_email_details_tuple
    dn_append = display_names.append
    ea_append = email_addresses.append

    for s in emails:
        dn, ea = parse_one(s)
        dn_append(dn)
        ea_append(ea)

    return display_names, email_addresses
