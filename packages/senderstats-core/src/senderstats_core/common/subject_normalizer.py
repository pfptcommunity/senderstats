from __future__ import annotations

from typing import Tuple

# from names_dataset import NameDataset
#
# _NAMEDATA = NameDataset()

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

# _RESOURCE_PACKAGE = "senderstats.common.data"
# _RESOURCE_NAME_SET = "global_names.pkl"
#
#
# def load_name_set() -> frozenset[str]:
#     try:
#         ref = resources.files(_RESOURCE_PACKAGE).joinpath(_RESOURCE_NAME_SET)
#         with ref.open("rb") as f:
#             return frozenset(pickle.load(f))
#     except FileNotFoundError:
#         return frozenset()

_EXTRA_WRAP_CHARS = "`*~^"
_TAIL_PUNCT = ",.;!?"
_STRIP_ALL = _STRIP_CHARS + _EXTRA_WRAP_CHARS

# Cheap edge-check set: only chars that trigger cleaning if seen at token edges.
# (We only need to detect, not remove.)
_EDGE_DIRTY = set(_STRIP_ALL + _TAIL_PUNCT)

_REL_TIME_UNITS = {
    "second", "seconds", "sec", "secs",
    "minute", "minutes", "min", "mins",
    "hour", "hours", "hr", "hrs",
    "day", "days",
    "month", "months",
    "year", "years", "yr", "yrs",
    "week", "weeks",
}

_PREFIX_WORDS = {
    # ─── Reply ──────────────────────────────────────────────
    "re",  # English / global default
    "aw",  # German (Antwort)
    "sv",  # Scandinavian (Svar)
    "vs",  # Finnish (Vastaus)
    "odp",  # Polish (Odpowiedź)
    "ynt",  # Turkish (Yanıt)
    "ré",  # French (rare but real)

    # ─── Forward ────────────────────────────────────────────
    "fw",  # English / Dutch
    "fwd",  # English
    "wg",  # German (Weitergeleitet)
    "tr",  # French (Transféré)
    "rv",  # Spanish (Reenviado)
    "reenv",  # Spanish (Reenviado)
    "enc",  # Portuguese (Encaminhado)
    "inoltro",  # Italian
    "pd",  # Polish (Przekaż dalej)
    "vb",  # Swedish (Vidarebefordrat)
    "vl",  # Finnish (Välitetty)
    "iletilen",  # Turkish (Forwarded)
    "转发",  # Chinese (Forwarded)
    "전달",  # Korean (Forwarded)

    # ─── Calendar / Scheduling Systems ──────────────────────
    "accepted",
    "declined",
    "tentative",
    "canceled",
    "cancelled",

    # Non-English calendar verbs
    "angenommen",  # German (Accepted)
    "abgelehnt",  # German (Declined)
    "aktualisiert",  # German (Updated)
    "mis à jour",  # French (Updated)
    "mise à jour",  # French variant
    "actualizado",  # Spanish (Updated)
    "actualizada",  # Spanish (gendered)
    "aggiornato",  # Italian (Updated)
    "aggiornata",

    # ─── System / Notification Noise ────────────────────────
    "updated",
    "invitation",
    "reminder",
    "notification",
    "alert",
    "notice",
}

_RESPONSE_PREFIXES = {word + ":" for word in _PREFIX_WORDS}

_IDENT_MARK_B = bytearray(256)
for ch in b"-_/\\:+@=#%&?~.":
    _IDENT_MARK_B[ch] = 1


def normalize_subject(subject: str) -> Tuple[str, bool]:
    if not subject:
        return "", False

    is_response: bool = False

    tokens = subject.split()
    n = len(tokens)
    strip_all = _STRIP_ALL
    tail_punct = _TAIL_PUNCT
    edge_dirty = _EDGE_DIRTY
    lower = str.lower

    S = [None] * n
    SL = [None] * n

    for idx, t in enumerate(tokens):
        if not t:
            S[idx] = ""
            SL[idx] = ""
            continue

        # Fast path: if edges are “clean”, avoid all stripping work.
        c0 = t[0]
        cN = t[-1]
        if (c0 not in edge_dirty) and (cN not in edge_dirty):
            s = t
        else:
            s = t.strip(strip_all)
            s = s.rstrip(tail_punct)
            s = s.strip(strip_all)

        S[idx] = s
        SL[idx] = lower(s) if s else ""

    # Handle leading response prefixes
    has_prefix = False
    i = 0
    while i < n:
        sl = SL[i]
        if sl in _PREFIX_WORDS and i + 1 < n and SL[i + 1] == ":":
            has_prefix = True
            i += 2
            continue
        elif sl in _RESPONSE_PREFIXES:
            has_prefix = True
            i += 1
            continue
        if ":" in sl:
            parts = sl.split(":")
            prefix_count = 0
            for p in parts:
                if p and p in _PREFIX_WORDS:
                    prefix_count += 1
                else:
                    break
            if prefix_count > 0:
                has_prefix = True
                remaining_sl = ":".join(parts[prefix_count:])
                if remaining_sl:
                    # Update S[i] and SL[i]
                    tok_parts = tokens[i].split(":")
                    if len(tok_parts) == len(parts):
                        remaining_tok = ":".join(tok_parts[prefix_count:])
                    else:
                        remaining_tok = remaining_sl  # fallback
                    S[i] = remaining_tok.strip(strip_all).rstrip(tail_punct).strip(strip_all)
                    SL[i] = lower(S[i])
                    break
                else:
                    i += 1
                    continue
        break

    out: list[str] = []
    append = out.append
    if has_prefix:
        is_response = True
        append("{r}")

    while i < n:
        tok = tokens[i]
        s = S[i]
        sl = SL[i]

        # Month-only replacement in normal text: "Meeting in October" -> "meeting in {m}"
        # Inlined _is_month_norm and _month_is_part_of_date
        if sl in _MONTHS and len(s) >= 3:
            # Check if part of date: month day... or day month...
            is_date_part = False
            if i + 1 < n:
                day_sl = SL[i + 1]
                if day_sl and ('0' <= day_sl[0] <= '9'):
                    if len(day_sl) >= 3 and day_sl[-2:] in ("st", "nd", "rd", "th"):
                        num = day_sl[:-2]
                    else:
                        num = day_sl
                    if num.isascii() and num.isdecimal():
                        d = int(num)
                        if 1 <= d <= 31:
                            is_date_part = True
            if not is_date_part and i > 0:
                day_sl = SL[i - 1]
                if day_sl and ('0' <= day_sl[0] <= '9'):
                    if len(day_sl) >= 3 and day_sl[-2:] in ("st", "nd", "rd", "th"):
                        num = day_sl[:-2]
                    else:
                        num = day_sl
                    if num.isascii() and num.isdecimal():
                        d = int(num)
                        if 1 <= d <= 31:
                            is_date_part = True
            if not is_date_part:
                append("{m}")
                i += 1
                continue

        if s and len(s) >= 16:
            c0 = s[0]
            if "0" <= c0 <= "9":
                # quick shape: YYYY-MM-DD[T ]...
                # Inlined _is_iso_date_token_s
                if len(s) >= 10:
                    date_part = s[:10]
                    if len(date_part) == 10:
                        sep = date_part[4]
                        if sep in "-/." and date_part[7] == sep:
                            y, m, d = date_part[0:4], date_part[5:7], date_part[8:10]
                            if (y.isascii() and y.isdecimal() and
                                m.isascii() and m.isdecimal() and
                                d.isascii() and d.isdecimal()):
                                yy, mm, dd = int(y), int(m), int(d)
                                # Inlined _valid_ymd
                                if 0 <= yy <= 9999 and 1 <= mm <= 12 and 1 <= dd <= 31:
                                    if not (mm in (4, 6, 9, 11) and dd > 30) and not (mm == 2 and dd > 29):
                                        if len(s) >= 16:
                                            sep = s[10]
                                            if sep in ("T", " "):
                                                # Inlined rough _is_time_token check for {t}
                                                time_part = s[11:]
                                                if _is_time_token_impl(time_part):
                                                    append("{t}")
                                                    i += 1
                                                    continue

        # Inlined _consume_date
        date_start_i = i
        if i < n and SL[i] in _DOW:  # optional DOW
            i += 1

        if i >= n:
            i = date_start_i  # reset if no date
            # continue to next checks
        else:
            a_s = S[i]
            a_sl = SL[i]

            # single-token ISO/numeric/compact date
            is_date = False
            # Inlined _is_iso_date_token_s
            if len(a_s) == 10:
                sep = a_s[4]
                if sep in "-/." and a_s[7] == sep:
                    y, m, d = a_s[0:4], a_s[5:7], a_s[8:10]
                    if (y.isascii() and y.isdecimal() and
                        m.isascii() and m.isdecimal() and
                        d.isascii() and d.isdecimal()):
                        yy, mm, dd = int(y), int(m), int(d)
                        if 0 <= yy <= 9999 and 1 <= mm <= 12 and 1 <= dd <= 31:
                            if not (mm in (4, 6, 9, 11) and dd > 30) and not (mm == 2 and dd > 29):
                                is_date = True
            if not is_date:
                # Inlined _is_numeric_date_token_s
                sep = None
                for ch in a_s:
                    if ch in "/-.":
                        sep = ch
                        break
                if sep:
                    parts = a_s.split(sep)
                    if len(parts) == 3:
                        a, b, c = parts
                        if (a and b and c and
                            a.isascii() and a.isdecimal() and
                            b.isascii() and b.isdecimal() and
                            c.isascii() and c.isdecimal()):
                            aa, bb, yy = int(a), int(b), int(c)
                            valid1 = 0 <= yy <= 9999 and 1 <= aa <= 12 and 1 <= bb <= 31
                            if valid1 and not (aa in (4, 6, 9, 11) and bb > 30) and not (aa == 2 and bb > 29):
                                is_date = True
                            else:
                                valid2 = 0 <= yy <= 9999 and 1 <= bb <= 12 and 1 <= aa <= 31
                                if valid2 and not (bb in (4, 6, 9, 11) and aa > 30) and not (bb == 2 and aa > 29):
                                    is_date = True
            if not is_date:
                # Inlined _is_compact_ymd_s
                if len(a_s) == 8 and a_s.isascii() and a_s.isdecimal():
                    y, m, d = a_s[0:4], a_s[4:6], a_s[6:8]
                    yy, mm, dd = int(y), int(m), int(d)
                    if 0 <= yy <= 9999 and 1 <= mm <= 12 and 1 <= dd <= 31:
                        if not (mm in (4, 6, 9, 11) and dd > 30) and not (mm == 2 and dd > 29):
                            is_date = True
            if is_date:
                append("{d}")
                i += 1
                j = _consume_datetime_after_date_impl(tokens, S, SL, i, n)
                if j > i:
                    out.pop()
                    append("{t}")
                    i = j
                continue

            # Month Day [Year]
            if a_sl in _MONTHS and i + 1 < n:
                day_sl = SL[i + 1]
                if day_sl and ('0' <= day_sl[0] <= '9'):
                    if len(day_sl) >= 3 and day_sl[-2:] in ("st", "nd", "rd", "th"):
                        num = day_sl[:-2]
                    else:
                        num = day_sl
                    if num.isascii() and num.isdecimal():
                        d = int(num)
                        if 1 <= d <= 31:
                            # optional year
                            if i + 2 < n:
                                ytok = SL[i + 2]
                                if (ytok.isascii() and ytok.isdecimal()) and (len(ytok) == 2 or len(ytok) == 4):
                                    append("{d}")
                                    i += 3
                                    j = _consume_datetime_after_date_impl(tokens, S, SL, i, n)
                                    if j > i:
                                        out.pop()
                                        append("{t}")
                                        i = j
                                    continue
                            append("{d}")
                            i += 2
                            j = _consume_datetime_after_date_impl(tokens, S, SL, i, n)
                            if j > i:
                                out.pop()
                                append("{t}")
                                i = j
                            continue

            # Day Month [Year]
            if a_sl and ('0' <= a_sl[0] <= '9'):
                if len(a_sl) >= 3 and a_sl[-2:] in ("st", "nd", "rd", "th"):
                    num = a_sl[:-2]
                else:
                    num = a_sl
                if num.isascii() and num.isdecimal():
                    d = int(num)
                    if 1 <= d <= 31 and i + 1 < n and SL[i + 1] in _MONTHS:
                        if i + 2 < n:
                            ytok = SL[i + 2]
                            if (ytok.isascii() and ytok.isdecimal()) and (len(ytok) == 2 or len(ytok) == 4):
                                append("{d}")
                                i += 3
                                j = _consume_datetime_after_date_impl(tokens, S, SL, i, n)
                                if j > i:
                                    out.pop()
                                    append("{t}")
                                    i = j
                                continue
                        append("{d}")
                        i += 2
                        j = _consume_datetime_after_date_impl(tokens, S, SL, i, n)
                        if j > i:
                            out.pop()
                            append("{t}")
                            i = j
                        continue

            i = date_start_i  # reset i if no date consumed

        # Drop pure punctuation tokens
        if not s:
            i += 1
            continue
        any_alnum = False
        for ch in s:
            o = ord(ch)
            if o < 256 and (_CLS[o] & 4):
                any_alnum = True
                break
        if not any_alnum:
            i += 1
            continue

        # Inlined _is_time_token_sl or _is_time_range_token
        if _is_time_or_range_impl(sl, s):
            base_j = i + 1
            if base_j < n and SL[base_j] in ("am", "pm"):
                base_j += 1
            dow_j = base_j
            if dow_j < n and SL[dow_j] in _DOW:
                dow_j += 1
            k = _consume_date_after_time_impl(tokens, S, SL, dow_j, n)
            if k > dow_j:
                append("{t}")
                i = k
                continue
            append("{tm}")
            i = base_j
            if i < n and SL[i] in _TZ:
                i += 1
            continue

        # Split AM/PM: "3" "pm" or "3:15" "PM"
        if i + 1 < n:
            suf = SL[i + 1]
            if suf in ("am", "pm"):
                glued = sl + suf
                if _is_time_token_impl(glued):
                    append("{tm}")
                    i += 2
                    if i < n and SL[i] in _TZ:
                        i += 1
                    continue

        # Inlined _consume_bare_duration: "24 hours" etc.
        if i + 1 < n and s.isascii() and s.isdecimal() and SL[i + 1] in _REL_TIME_UNITS:
            append("{t}")
            i += 2
            continue

        # Integer
        if s.isascii() and s.isdecimal():
            append("{i}")
            i += 1
            continue

        if tok.endswith(":"):
            head = tok[:-1]
            head_stripped = head.strip(_STRIP_CHARS)
            if head_stripped and head_stripped.isalpha():
                append(head_stripped.lower() + ":")
                i += 1
                continue

        # Inlined _looks_identifier
        if s.isalpha():
            pass  # false
        else:
            any_a = any_d = any_sym = has_mark = False
            for ch in s:
                o = ord(ch)
                if o < 256:
                    c = _CLS[o]
                    if c & 1:
                        any_a = True
                    elif c & 2:
                        any_d = True
                    elif c & 16:
                        if _IDENT_MARK_B[o]:
                            has_mark = True
                else:
                    any_sym = True
                if has_mark and (any_a or any_d):
                    break  # early exit
            if (any_a or any_d) and ((any_a and any_d) or has_mark or (any_sym and (any_a or any_d))):
                append("{#}")
                i += 1
                continue

        # if tok in _NAMEDATA.first_names:
        #     append('{f}')
        #     i += 1
        #     continue
        #
        # if tok in _NAMEDATA.last_names:
        #     append('{l}')
        #     i += 1
        #     continue

        # Normal word/token
        append(tok.lower())
        i += 1

    return " ".join(out), is_response


# Helper impls for inlined time/date logic (kept separate to avoid bloating main loop)
def _is_time_token_impl(t: str) -> bool:
    if not t:
        return False
    suffix = ""
    if t.endswith("am") or t.endswith("pm"):
        suffix = t[-2:]
        t = t[:-2]
        if not t:
            return False
    if ":" not in t:
        if suffix and t.isascii() and t.isdecimal() and 1 <= int(t) <= 12:
            return True
        return False
    c1 = t.find(":")
    if c1 <= 0:
        return False
    hh = t[:c1]
    rest = t[c1 + 1:]
    if not (len(hh) <= 2 and hh.isascii() and hh.isdecimal()):
        return False
    if len(rest) < 2 or not (rest[:2].isascii() and rest[:2].isdecimal()):
        return False
    mm = int(rest[:2])
    ss = None
    if len(rest) > 2:
        if len(rest) != 5 or rest[2] != ":" or not (rest[3:5].isascii() and rest[3:5].isdecimal()):
            return False
        ss = int(rest[3:5])
        if ss > 59:
            return False
    h = int(hh)
    if suffix:
        if h < 1 or h > 12:
            return False
    else:
        if h > 23:
            return False
    return mm <= 59


def _is_time_or_range_impl(sl: str, s: str) -> bool:
    dash = s.find("-")
    if dash > 0 and dash < len(s) - 1:
        a = s[:dash].strip().lower()
        b = s[dash + 1:].strip().lower()
        if _is_time_token_impl(a) and _is_time_token_impl(b):
            return True
    # time
    suffix = ""
    if sl.endswith("am") or sl.endswith("pm"):
        suffix = sl[-2:]
        sl = sl[:-2]
        if not sl:
            return False
    if ":" not in sl:
        if suffix and sl.isascii() and sl.isdecimal() and 1 <= int(sl) <= 12:
            return True
        return False
    c1 = sl.find(":")
    if c1 <= 0:
        return False
    hh = sl[:c1]
    rest = sl[c1 + 1:]
    if not (len(hh) <= 2 and hh.isascii() and hh.isdecimal()):
        return False
    if len(rest) < 2 or not (rest[:2].isascii() and rest[:2].isdecimal()):
        return False
    mm = int(rest[:2])
    if len(rest) > 2:
        if len(rest) != 5 or rest[2] != ":" or not (rest[3:5].isascii() and rest[3:5].isdecimal()):
            return False
        if int(rest[3:5]) > 59:
            return False
    h = int(hh)
    if suffix:
        if h < 1 or h > 12:
            return False
    else:
        if h > 23:
            return False
    if mm > 59:
        return False
    return True


def _consume_datetime_after_date_impl(tokens: list[str], S: list[str], SL: list[str], i: int, n: int) -> int:
    if i >= n or SL[i] == "at":
        return i
    # consume time or range
    t0_s = S[i]
    t0_sl = SL[i]
    if _is_time_or_range_impl(t0_sl, t0_s):
        j = i + 1
    elif i + 1 < n and SL[i + 1] in ("am", "pm") and (":" in t0_sl or (t0_sl.isascii() and t0_sl.isdecimal())):
        glued = t0_sl + SL[i + 1]
        if _is_time_token_impl(glued):
            j = i + 2
        else:
            return i
    else:
        return i
    # eat am/pm/tz
    if j < n and SL[j] in ("am", "pm"):
        j += 1
    if j < n and SL[j] in _TZ:
        j += 1
    # optional range "-"
    if j < n and S[j] == "-":
        if j + 1 >= n:
            return i
        t1_s = S[j + 1]
        t1_sl = SL[j + 1]
        if _is_time_or_range_impl(t1_sl, t1_s):
            j += 2
        elif j + 2 < n and SL[j + 2] in ("am", "pm") and (":" in t1_sl or (t1_sl.isascii() and t1_sl.isdecimal())):
            glued = t1_sl + SL[j + 2]
            if _is_time_token_impl(glued):
                j += 3
            else:
                return i
        else:
            return i
        # eat am/pm/tz again
        if j < n and SL[j] in ("am", "pm"):
            j += 1
        if j < n and SL[j] in _TZ:
            j += 1
    # optional paren tz
    if j < n and SL[j] in _TZ:
        j += 1
    return j


def _consume_date_after_time_impl(tokens: list[str], S: list[str], SL: list[str], i: int, n: int) -> int:
    # Similar to _consume_date, but returns new i or old i
    date_start_i = i
    if i < n and SL[i] in _DOW:  # optional DOW
        i += 1
    if i >= n:
        return date_start_i
    a_s = S[i]
    a_sl = SL[i]
    is_date = False
    # ISO
    if len(a_s) == 10:
        sep = a_s[4]
        if sep in "-/." and a_s[7] == sep:
            y, m, d = a_s[0:4], a_s[5:7], a_s[8:10]
            if (y.isascii() and y.isdecimal() and
                m.isascii() and m.isdecimal() and
                d.isascii() and d.isdecimal()):
                yy, mm, dd = int(y), int(m), int(d)
                if 0 <= yy <= 9999 and 1 <= mm <= 12 and 1 <= dd <= 31:
                    if not (mm in (4, 6, 9, 11) and dd > 30) and not (mm == 2 and dd > 29):
                        is_date = True
    if not is_date:
        # Numeric
        sep = None
        for ch in a_s:
            if ch in "/-.":
                sep = ch
                break
        if sep:
            parts = a_s.split(sep)
            if len(parts) == 3:
                a, b, c = parts
                if (a and b and c and
                    a.isascii() and a.isdecimal() and
                    b.isascii() and b.isdecimal() and
                    c.isascii() and c.isdecimal()):
                    aa, bb, yy = int(a), int(b), int(c)
                    valid1 = 0 <= yy <= 9999 and 1 <= aa <= 12 and 1 <= bb <= 31
                    if valid1 and not (aa in (4, 6, 9, 11) and bb > 30) and not (aa == 2 and bb > 29):
                        is_date = True
                    else:
                        valid2 = 0 <= yy <= 9999 and 1 <= bb <= 12 and 1 <= aa <= 31
                        if valid2 and not (bb in (4, 6, 9, 11) and aa > 30) and not (bb == 2 and aa > 29):
                            is_date = True
    if not is_date:
        # Compact
        if len(a_s) == 8 and a_s.isascii() and a_s.isdecimal():
            y, m, d = a_s[0:4], a_s[4:6], a_s[6:8]
            yy, mm, dd = int(y), int(m), int(d)
            if 0 <= yy <= 9999 and 1 <= mm <= 12 and 1 <= dd <= 31:
                if not (mm in (4, 6, 9, 11) and dd > 30) and not (mm == 2 and dd > 29):
                    is_date = True
    if is_date:
        return i + 1  # consume the date token
    # Month Day [Year]
    if a_sl in _MONTHS and i + 1 < n:
        day_sl = SL[i + 1]
        if day_sl and ('0' <= day_sl[0] <= '9'):
            if len(day_sl) >= 3 and day_sl[-2:] in ("st", "nd", "rd", "th"):
                num = day_sl[:-2]
            else:
                num = day_sl
            if num.isascii() and num.isdecimal():
                d = int(num)
                if 1 <= d <= 31:
                    if i + 2 < n:
                        ytok = SL[i + 2]
                        if (ytok.isascii() and ytok.isdecimal()) and (len(ytok) == 2 or len(ytok) == 4):
                            return i + 3
                    return i + 2
    # Day Month [Year]
    if a_sl and ('0' <= a_sl[0] <= '9'):
        if len(a_sl) >= 3 and a_sl[-2:] in ("st", "nd", "rd", "th"):
            num = a_sl[:-2]
        else:
            num = a_sl
        if num.isascii() and num.isdecimal():
            d = int(num)
            if 1 <= d <= 31 and i + 1 < n and SL[i + 1] in _MONTHS:
                if i + 2 < n:
                    ytok = SL[i + 2]
                    if (ytok.isascii() and ytok.isdecimal()) and (len(ytok) == 2 or len(ytok) == 4):
                        return i + 3
                return i + 2
    return date_start_i
