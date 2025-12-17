from __future__ import annotations

import pickle
from importlib import resources
from typing import Optional, Any, Set

_CLS = bytearray(256)
for o in range(256):
    if 48 <= o <= 57:  # 0-9
        _CLS[o] = 2 | 4  # digit + alnum
    elif 65 <= o <= 90:  # A-Z
        _CLS[o] = 1 | 4  # alpha + alnum
    elif 97 <= o <= 122:  # a-z
        _CLS[o] = 1 | 4
    elif o in (9, 10, 11, 12, 13, 32):
        _CLS[o] = 8  # whitespace
    else:
        _CLS[o] = 16  # other

_MONTHS = {
    "jan", "january", "feb", "february", "mar", "march", "apr", "april", "may",
    "jun", "june", "jul", "july", "aug", "august", "sep", "sept", "september",
    "oct", "october", "nov", "november", "dec", "december"
}
_DOW = {
    "mon", "monday", "tue", "tues", "tuesday", "wed", "wednesday",
    "thu", "thur", "thurs", "thursday", "fri", "friday", "sat", "saturday",
    "sun", "sunday"
}

_TZ = {
    "utc", "gmt", "est", "edt", "cst", "cdt", "mst", "mdt", "pst", "pdt"
}

_STRIP_CHARS = "[](){}<>,.;!\"'"

_RESOURCE_PACKAGE = "senderstats.common.data"
_RESOURCE_NAME_SET = "global_names.pkl"

def load_name_set() -> frozenset[str]:
    try:
        ref = resources.files(_RESOURCE_PACKAGE).joinpath(_RESOURCE_NAME_SET)
        with ref.open("rb") as f:
            return frozenset(pickle.load(f))
    except FileNotFoundError:
        return frozenset()

def _strip_wrap(tok: str) -> str:
    return tok.strip(_STRIP_CHARS)


def _is_month_norm(sl: str) -> bool:
    return sl in _MONTHS


def _is_dow_norm(sl: str) -> bool:
    return sl in _DOW


def _is_tz_norm(sl: str) -> bool:
    return sl in _TZ


def _parse_ordinal_day_norm(sl: str) -> int | None:
    # sl is already strip+lower+rstrip(",")
    if not sl:
        return None
    # fast reject: must start with digit
    c0 = sl[0]
    if c0 < "0" or c0 > "9":
        return None
    if len(sl) >= 3 and sl[-2:] in ("st", "nd", "rd", "th"):
        num = sl[:-2]
    else:
        num = sl
    if not _is_all_digits(num):
        return None
    d = _parse_upto_4_digits(num)
    return d if 1 <= d <= 31 else None


def _is_all_digits(s: str) -> bool:
    return bool(s) and s.isdigit()


def _parse_upto_4_digits(s: str) -> int:
    # s is ASCII digits, length 1..4
    v = 0
    for ch in s:
        v = v * 10 + (ord(ch) - 48)
    return v


def _valid_ymd(y: int, m: int, d: int) -> bool:
    # Your tests include year 0000 and 9999, so accept wide range.
    if y < 0 or y > 9999:
        return False
    if m < 1 or m > 12:
        return False
    if d < 1 or d > 31:
        return False
    if m in (4, 6, 9, 11) and d > 30:
        return False
    if m == 2 and d > 29:
        return False
    return True


def _is_time_token_sl(sl: str) -> bool:
    # sl is already stripped+lower+rstrip(",") (SL[i])
    if not sl:
        return False

    suffix = ""
    if sl.endswith("am") or sl.endswith("pm"):
        suffix = sl[-2:]
        sl = sl[:-2]
        if not sl:
            return False

    if ":" not in sl:
        if suffix and sl.isdigit():
            h = _parse_upto_4_digits(sl)
            return 1 <= h <= 12
        return False

    c1 = sl.find(":")
    if c1 <= 0:
        return False
    hh = sl[:c1]
    rest = sl[c1 + 1:]
    if not (hh.isdigit() and len(hh) <= 2):
        return False
    if len(rest) < 2 or not rest[:2].isdigit():
        return False
    mm = _parse_upto_4_digits(rest[:2])

    if len(rest) != 2:
        if len(rest) != 5 or rest[2] != ":" or not rest[3:5].isdigit():
            return False
        ss = _parse_upto_4_digits(rest[3:5])
        if ss > 59:
            return False

    h = _parse_upto_4_digits(hh)
    if suffix:
        if h < 1 or h > 12:
            return False
    else:
        if h > 23:
            return False

    return mm <= 59


def _is_compact_ymd_s(t: str) -> bool:
    if len(t) != 8 or not t.isdigit():
        return False
    y = _parse_upto_4_digits(t[0:4])
    m = _parse_upto_4_digits(t[4:6])
    d = _parse_upto_4_digits(t[6:8])
    return _valid_ymd(y, m, d)


def _is_iso_date_token_s(t: str) -> bool:
    if len(t) != 10:
        return False
    sep = t[4]
    if sep not in "-/." or t[7] != sep:
        return False
    y, m, d = t[0:4], t[5:7], t[8:10]
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        return False
    return _valid_ymd(_parse_upto_4_digits(y), _parse_upto_4_digits(m), _parse_upto_4_digits(d))


def _is_numeric_date_token_s(t: str) -> bool:
    # t already stripped
    sep = None
    for ch in t:
        if ch in "/-.":
            sep = ch
            break
    if not sep:
        return False
    parts = t.split(sep)
    if len(parts) != 3:
        return False
    a, b, c = parts
    if not (a and b and c and a.isdigit() and b.isdigit() and c.isdigit()):
        return False
    aa = _parse_upto_4_digits(a)
    bb = _parse_upto_4_digits(b)
    yy = _parse_upto_4_digits(c)
    return _valid_ymd(yy, aa, bb) or _valid_ymd(yy, bb, aa)


def _is_iso_datetime_token(tok: str) -> bool:
    t = tok.strip(_STRIP_CHARS)
    if len(t) < 16:
        return False
    if not _is_iso_date_token_s(t[:10]):  # no extra strip
        return False
    sep = t[10]
    if sep != "T" and sep != " ":
        return False
    return _is_time_token(t[11:])


def _is_time_token(tok: str) -> bool:
    # Supports:
    # - HH:MM
    # - HH:MM:SS
    # - Hpm / HHpm / H:MMpm / H:MM pm (the separated "pm" is handled elsewhere)
    t = _strip_wrap(tok).lower().rstrip(",")
    if not t:
        return False

    # peel am/pm suffix if present
    suffix = ""
    if t.endswith("am") or t.endswith("pm"):
        suffix = t[-2:]
        t = t[:-2]
        if not t:
            return False

    # pure hour like "2" is not a time token here (we only treat "2pm" etc)
    # so require either ":" or suffix+digits handled below.
    if ":" not in t:
        # allow "2pm" / "12am" case (suffix already peeled)
        if suffix and _is_all_digits(t) and 1 <= _parse_upto_4_digits(t) <= 12:
            return True
        return False

    c1 = t.find(":")
    if c1 <= 0:
        return False
    hh = t[:c1]
    rest = t[c1 + 1:]
    if not (_is_all_digits(hh) and len(hh) <= 2):
        return False
    if len(rest) < 2 or not _is_all_digits(rest[:2]):
        return False
    mm = _parse_upto_4_digits(rest[:2])

    ss = None
    if len(rest) == 2:
        pass
    else:
        if len(rest) != 5 or rest[2] != ":" or not _is_all_digits(rest[3:5]):
            return False
        ss = _parse_upto_4_digits(rest[3:5])

    h = _parse_upto_4_digits(hh)
    if suffix:
        if h < 1 or h > 12:
            return False
    else:
        if h < 0 or h > 23:
            return False
    if mm > 59:
        return False
    if ss is not None and ss > 59:
        return False
    return True


def _is_time_range_token(tok: str) -> bool:
    # "2pm-3pm" or "14:00-15:30"
    t = _strip_wrap(tok).lower()
    dash = t.find("-")
    if dash <= 0 or dash >= len(t) - 1:
        return False
    a = t[:dash].strip()
    b = t[dash + 1:].strip()
    return _is_time_token(a) and _is_time_token(b)


_IDENT_MARK_B = bytearray(256)
for ch in b"-_./\\:+@=#%&?~":
    _IDENT_MARK_B[ch] = 1


def _looks_identifier(tok: str) -> bool:
    if tok.isalpha():
        return False
    # identifier-ish: mix of alpha+digit, or symbols mixed with alnum, or contains ident marks
    any_a = any_d = any_sym = has_mark = False
    CLS = _CLS

    for ch in tok:
        o = ord(ch)
        if o < 256:
            c = CLS[o]
            if c & 1:
                any_a = True
            elif c & 2:
                any_d = True
            elif c & 16:
                any_sym = True
                if _IDENT_MARK_B[o]:
                    has_mark = True
        else:
            any_sym = True

        # OPTIONAL early exit (small win)
        if has_mark and (any_a or any_d):
            return True

    if not (any_a or any_d):
        return False

    return (any_a and any_d) or has_mark or (any_sym and (any_a or any_d))


def _month_is_part_of_date(SL: list[str], i: int) -> bool:
    # month day...
    if i + 1 < len(SL) and _parse_ordinal_day_norm(SL[i + 1]) is not None:
        return True
    # day month...
    if i > 0 and _parse_ordinal_day_norm(SL[i - 1]) is not None:
        return True
    return False


def _consume_date(tokens: list[str], S: list[str], SL: list[str], i: int) -> tuple[bool, int]:
    """If tokens[i..] starts with a date (possibly with DOW), consume it and return (True, new_i)."""
    n = len(tokens)

    # optional DOW (use SL)
    if i < n and _is_dow_norm(SL[i]):
        i += 1

    if i >= n:
        return False, i

    a_s = S[i]
    a_sl = SL[i]

    # single-token ISO/numeric/compact date (use stripped string to reduce strip churn a bit)
    if _is_iso_date_token_s(a_s) or _is_numeric_date_token_s(a_s) or _is_compact_ymd_s(a_s):
        return True, i + 1

    # Month Day [Year]
    if _is_month_norm(a_sl) and i + 1 < n:
        day = _parse_ordinal_day_norm(SL[i + 1])
        if day is not None:
            # optional year (2 or 4 digits)
            if i + 2 < n:
                ytok = SL[i + 2]  # already stripped+lower+rstrip(",")
                if _is_all_digits(ytok) and (len(ytok) == 2 or len(ytok) == 4):
                    return True, i + 3
            return True, i + 2

    # Day Month [Year]
    day = _parse_ordinal_day_norm(a_sl)
    if day is not None and i + 1 < n and _is_month_norm(SL[i + 1]):
        if i + 2 < n:
            ytok = SL[i + 2]
            if _is_all_digits(ytok) and (len(ytok) == 2 or len(ytok) == 4):
                return True, i + 3
        return True, i + 2

    return False, i


def _consume_datetime_after_date(tokens: list[str], S: list[str], SL: list[str], i: int) -> tuple[bool, int]:
    """
    Assumes i is positioned at the first token AFTER a consumed date.
    Consumes:
      - time
      - optional range ("-" time) or single-token "time-time"
      - optional timezone tokens, including "(EST)" etc
    Returns (True, new_i) if a time/time-range was consumed.
    """
    is_time_token = _is_time_token
    is_all_digits = _is_all_digits
    is_tz_norm = _is_tz_norm
    is_time_token_sl = _is_time_token_sl
    is_time_range_token = _is_time_range_token

    def eat_ampm_tz(j: int) -> int:
        if j < n and SL[j] in ("am", "pm"):
            j += 1
        if j < n and is_tz_norm(SL[j]):
            j += 1
        return j

    n = len(tokens)
    if i >= n:
        return False, i

    # If "at" appears, do NOT treat it as datetime (matches your "… at 2:30pm" => {#})
    if SL[i] == "at":
        return False, i

    # time can be in one token, OR split as ["2", "pm"]
    def consume_time(j: int) -> tuple[bool, int]:
        if j >= n:
            return False, j

        t0_s = S[j]
        t0_sl = SL[j]

        if is_time_token_sl(t0_sl) or is_time_range_token(t0_s):
            return True, j + 1

        # split am/pm: "2" "pm" or "2:30" "pm"
        core = t0_sl
        if (core and (":" in core or is_all_digits(core))) and j + 1 < n:
            suf = SL[j + 1]
            if suf in ("am", "pm"):
                glued = core + suf
                if is_time_token(glued):
                    return True, j + 2

        return False, j

    ok, j = consume_time(i)
    if not ok:
        return False, i

    j = eat_ampm_tz(j)

    # optional range: "-" then another time (or token like "2pm-3pm" already handled)
    if j < n and S[j] == "-":
        ok2, j2 = consume_time(j + 1)
        if ok2:
            j = eat_ampm_tz(j2)

    # optional parenthesized tz like "(EST)"
    if j < n and is_tz_norm(SL[j]):
        j += 1

    return True, j


def normalize_subject(subject: str) -> str:
    CLS = _CLS
    is_month_norm = _is_month_norm
    month_is_part_of_date = _month_is_part_of_date
    is_iso_datetime_token = _is_iso_datetime_token
    consume_date = _consume_date
    consume_datetime_after_date = _consume_datetime_after_date
    is_time_token_sl = _is_time_token_sl
    is_dow_norm = _is_dow_norm
    is_tz_norm = _is_tz_norm
    is_all_digits = _is_all_digits
    looks_identifier = _looks_identifier
    is_time_token = _is_time_token

    def _eat_tz(j: int) -> int:
        if j < n and is_tz_norm(SL[j]):
            return j + 1
        return j

    def _eat_ampm_tz(j: int) -> int:
        if j < n and SL[j] in ("am", "pm"):
            j += 1
        if j < n and is_tz_norm(SL[j]):
            j += 1
        return j

    if not subject:
        return ""

    # tokens = subject.split()
    # S = [t.strip(_STRIP_CHARS) for t in tokens]
    # SL = [s.lower().rstrip(",") if s else "" for s in S]
    stripchars = _STRIP_CHARS
    tokens = subject.split()
    S = [t.strip(stripchars) for t in tokens]
    SL = [s.lower().rstrip(",") if s else "" for s in S]

    out: list[str] = []
    append = out.append

    i = 0
    n = len(tokens)

    while i < n:
        tok = tokens[i]
        s = S[i]
        sl = SL[i]

        # Month-only replacement in normal text:
        # "Meeting in October" -> "meeting in {m}"
        if is_month_norm(sl) and len(s) >= 3 and not month_is_part_of_date(SL, i):
            append("{m}")
            i += 1
            continue

        # Standalone ISO datetime token like "2025-12-11T14:22:33"
        if is_iso_datetime_token(tok):
            append("{t}")
            i += 1
            continue

        # Date (possibly with DOW) and optional attached time/range
        ok_date, j = consume_date(tokens, S, SL, i)
        if ok_date:
            # if a time follows immediately, collapse to {t}; else {d}
            ok_dt, k = consume_datetime_after_date(tokens, S, SL, j)
            if ok_dt:
                append("{t}")
                i = k
            else:
                append("{d}")
                i = j
            continue

        # If token itself is a time range (rare, but you have "2pm-3pm" case after date)
        # Outside date context, your tests expect time-like things to become {#}, not {t}.
        # So we intentionally do NOT emit {t} here.

        # Drop pure punctuation tokens
        if not s:
            i += 1
            continue
        any_alnum = False
        for ch in s:
            o = ord(ch)
            if o < 256 and (CLS[o] & 4):
                any_alnum = True
                break
        if not any_alnum:
            i += 1
            continue

        core = sl

        if is_time_token_sl(sl):
            j = i + 1

            # allow optional standalone AM/PM token (rare here, but consistent)
            if j < n:
                s = SL[j]
                if s in ("am", "pm"):
                    j += 1

            # allow optional DOW token ("Mon," etc.)
            if j < n and is_dow_norm(SL[j]):
                j += 1

            ok_date, k = consume_date(tokens, S, SL, j)
            if ok_date:
                append("{t}")
                i = k
                continue

            # otherwise it's time-only
            append("{tm}")
            i = _eat_ampm_tz(i + 1)
            continue

        # Split AM/PM: "3" "pm" or "3:15" "PM"
        if i + 1 < n:
            suf = SL[i + 1]
            if suf in ("am", "pm"):
                glued = core + suf
                if is_time_token(glued):
                    append("{tm}")
                    i = _eat_tz(i + 2)
                    continue

        # Integer
        if is_all_digits(s):
            append("{i}")
            i += 1
            continue

        if tok.endswith(":"):
            head = tok[:-1]
            head_stripped = _strip_wrap(head)
            if head_stripped and head_stripped.isalpha():
                append(head_stripped.lower() + ":")
                i += 1
                continue

        # Identifier-ish
        if looks_identifier(s):
            append("{#}")
            i += 1
            continue

        # Normal word/token
        append(tok.lower())
        i += 1

    return " ".join(out)
