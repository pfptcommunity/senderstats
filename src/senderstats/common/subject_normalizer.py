import os
import pickle

import re2  # Superfast, not good for back tracking
import regex as re # Used for ID Matching

# -------------------------------
# Pattern definitions
# -------------------------------

# Email Address Pattern
EMAIL_REGEX = r"\b[A-Za-z0-9.!#$%&'*+\/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\b"

# Year: 4-digit or 2-digit (rejects 3-digit years)
YEAR_REGEX = r"(?:\d{4}|\d{2})"

# Day-of-month used with month names (1–31, optional leading 0).
DAY_TEXT = r"(?:[12]\d|3[01]|0?[1-9])"

# Numeric day/month (1–31 / 1–12, optional leading 0).
DAY_NUM = DAY_TEXT
MON_NUM = r"(?:1[0-2]|0?[1-9])"

# Month names (short + long), lowercased; we use (?i) in the regex for case-insensitive.
MONTH_NAME_REGEX = (
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|"
    r"may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|"
    r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)

# Day-of-week names (short + long)
DOW_NAME_REGEX = (
    r"(?:mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|"
    r"fri(?:day)?|sat(?:urday)?|sun(?:day)?)"
)

# Time (with optional minutes/seconds, optional am/pm)
TIME_PART_REGEX = r"\d{1,2}(?::\d{2}(?::\d{2})?)?\s*(?:am|pm)?"

# Optional time or time range with optional timezone in parentheses.
TIME_RANGE_PART = (
        r"(?:[ t]+" + TIME_PART_REGEX +  # first time, after space or "T"
        r"(?:\s*-\s*" + TIME_PART_REGEX + r")?"  # optional " - second time"
                                          r"(?:\s*(?:\([A-Z]{2,5}\)|[A-Z]{2,5}))?"  # optional "(EST)" or "UTC"/"PST"
                                          r")?"  # whole time/range is optional
)

DATE_FORMS_REGEX = (
        r"(?:"
        # 1) ISO / YMD with -, / or .  => 2025-12-05, 2025/12/05, 2025.12.05
        r"\d{4}[\/\-.](?:0[1-9]|1[0-2])[\/\-.](?:0[1-9]|[12]\d|3[01])"
        r"|"

        # 2) Numeric DMY or MDY with / - . (1- or 2-digit day/month)
        #    Examples: 12/11/2025, 12-11-25, 1/2/25, 1/2/2025, 10.12.25
        r"(?:" + DAY_NUM + r"[\/\-\.]" + MON_NUM +
        r"|" + MON_NUM + r"[\/\-\.]" + DAY_NUM +
        r")"
        r"[\/\-\.]" + YEAR_REGEX +
        r"|"

        # 3) Day Month [Year] (with optional ordinal + optional comma before year)
        #    11 Dec 2025, 11 Dec, 2025, 11th Dec 2025, 11th Dec, 2025, 11 Dec
        r"(?:[12]\d|3[01]|0?[1-9])(?:st|nd|rd|th)?\s+" + MONTH_NAME_REGEX +
        r"(?:,?\s+" + YEAR_REGEX + r")?"
                                   r"|"

        # 4) Month[, ] Day [Year]
        #    Dec 11, 2025 / Dec, 11, 2025 / Dec 11 / December 11 2025 / Dec, 24
                                   r"" + MONTH_NAME_REGEX +
        r",?\s+" + DAY_TEXT +
        r"(?:"
        r",\s*" + YEAR_REGEX +
        r"|"
        r"\s+" + YEAR_REGEX +
        r")?"
        r")"
)

COMBINED_DATE_REGEX = (
        r"(?i)"
        r"(?:"
        # 1) DOW [optional] + DATE_FORMS + optional time/range
        r"(?:\b" + DOW_NAME_REGEX + r",?\s+)?"
        + DATE_FORMS_REGEX +
        TIME_RANGE_PART +
        r"|"
        # 2) TIME first, then optional DOW, then DATE_FORMS + optional time/range
        + TIME_PART_REGEX +
        r"\s+(?:\b" + DOW_NAME_REGEX + r",?\s+)?" +
        DATE_FORMS_REGEX +
        TIME_RANGE_PART +
        r"|"
        # 3) Standalone month name
        r"\b" + MONTH_NAME_REGEX + r"\b"
                                   r")"
)

# ID-like token matcher, using `regex` because of lookbehinds / complex negatives.
IDENTIFIER_REGEX = (
    r"(?<!\S)"
    r"(?!\{[a-z_]+\}(?!\S))"
    r"(?![.,!?;:]*[A-Za-z]+(?:['’][A-Za-z]+)*[.,!?;:]*(?!\S))"
    r"(?!\d+(?!\S))"
    r"(?![^\w\s'\"]+(?!\S))"
    r"\S+"
)


# -------------------------------
# SubjectNormalizer
# -------------------------------

class SubjectNormalizer:
    """
    Configurable subject normalizer.

    Core responsibilities:
      - Date/datetime detection and replacement ({d}, {t})
      - Standalone month detection ({m})
      - Email detection ({e})
      - ID-like token detection ({#})
      - Integer literal detection ({i})
      - Optional name replacement via Aho–Corasick ({n})

    You can pass a custom name automaton (e.g., customer-specific name list),
    or disable names entirely.
    """

    def __init__(
            self,
            name_automaton=None,
            enable_names: bool = False,
    ) -> None:
        """
        Args:
            name_automaton:
                Aho–Corasick automaton that supports `iter(text)` and yields
                (end_index, matched_name) pairs. If None, name replacement is disabled.
            enable_names:
                Whether to enable name replacement. Only effective if
                `name_automaton` is not None.
        """
        self.name_automaton = name_automaton
        self.enable_names = enable_names and (name_automaton is not None)

        # Compile regexes once per instance (cheap; typically you have 1 instance).
        self.combined_date_re = re2.compile(COMBINED_DATE_REGEX)
        self.identifier_re = re.compile(IDENTIFIER_REGEX)
        self.email_address_re = re.compile(EMAIL_REGEX)
        self.month_only_re = re2.compile(r"(?i)\b" + MONTH_NAME_REGEX + r"\b")

    # ---- internal helpers ----

    def _replace_months(self, s: str) -> str:
        """Replace standalone month names with {m}."""
        return self.month_only_re.sub("{m}", s)

    def _replace_names(self, subject: str) -> str:
        """
        Replace name occurrences in a subject using the configured Aho–Corasick
        automaton, respecting word boundaries. If no automaton is configured or
        names are disabled, the input is returned unchanged.
        """
        if not self.enable_names or self.name_automaton is None:
            return subject

        A = self.name_automaton
        subj = subject
        matches = []

        # Collect candidate match spans from the automaton
        for end, name in A.iter(subj):
            start = end - len(name) + 1

            # left boundary: preceding char must not be alphanumeric
            if start > 0 and subject[start - 1].isalnum():
                continue

            # right boundary: following char must not be alphanumeric
            if end + 1 < len(subject) and subject[end + 1].isalnum():
                continue

            matches.append((start, end))

        if not matches:
            return subject

        # merge overlapping spans
        matches.sort()
        merged = []
        prev = matches[0]

        for curr in matches[1:]:
            if curr[0] <= prev[1] + 1:
                prev = (prev[0], max(prev[1], curr[1]))
            else:
                merged.append(prev)
                prev = curr
        merged.append(prev)

        # build final replaced string
        result = []
        last = 0

        for start, end in merged:
            result.append(subject[last:start])
            result.append("{n}")
            last = end + 1

        result.append(subject[last:])
        return "".join(result)

    def _replace_dates(self, s: str) -> str:
        """
        One-pass date/datetime/month replacement using RE2.

        Classification:
          - contains clock time or am/pm  => {t}
          - contains digits (no time)     => {d}
          - no digits                     => {m}
        """
        out = []
        i = 0

        for m in self.combined_date_re.finditer(s):
            start, end = m.span()
            if start > i:
                out.append(s[i:start])

            span_text = s[start:end]
            lt = span_text.lower()

            if re.search(r"\d{1,2}:\d{2}", lt) or "am" in lt or "pm" in lt:
                out.append("{t}")
            elif re.search(r"\d", lt):
                out.append("{d}")
            else:
                out.append("{m}")

            i = end

        if i < len(s):
            out.append(s[i:])

        return "".join(out)

    # ---- public API ----

    def normalize(self, subj: str) -> str:
        """
        Normalize a raw subject into a compact, templated representation.

        Pipeline:
          1) Strip whitespace
          2) {t}/{d}/{m} via _replace_dates
          3) Emails  -> {e}
          4) IDs     -> {#}
          5) Ints    -> {i}
          6) Names   -> {n} (optional, if enabled)
          7) Standalone months -> {m}
          8) Collapse whitespace
          9) Lowercase
        """
        s = subj.strip()
        s = self._replace_dates(s)
        s = self.email_address_re.sub("{e}", s)
        s = self.identifier_re.sub("{#}", s)
        s = re.sub(r"\b\d+\b", "{i}", s)
        s = self._replace_names(s)
        s = self._replace_months(s)
        s = re.sub(r"\s+", " ", s)
        return s.lower()


# -------------------------------
# Test cases
# -------------------------------

iso_tests = {
    # originals
    "2025-12-11": "{d}",
    "2025-12-11 14:22": "{t}",
    "2025-12-11T14:22": "{t}",
    "2025-12-11T14:22:33": "{t}",

    # extra ISO / YMD variants
    "0001-01-01": "{d}",
    "9999-12-31": "{d}",
    "2025/12/11": "{d}",
    "2025/12/11 23:59": "{t}",
    "2025.12.11 23:59:59": "{t}",
}

numeric_date_tests = {
    # originals
    "12/11/2025": "{d}",
    "12-11-25": "{d}",
    "1/2/25": "{d}",
    "1/2/2025 14:00": "{t}",

    # more numeric variants
    "10.2.25": "{d}",
    "01.12.0000": "{d}",
    "12/11/0001": "{d}",
    "3-1-99": "{d}",
    "03-01-1999 08:15": "{t}",
    "10.12.2025 08:15 - 09:00": "{t}",
}

month_day_year_tests = {
    # originals
    "Dec 11, 2025": "{d}",
    "Dec 11 2025": "{d}",
    "Dec 11": "{d}",
    "Dec, 11": "{d}",
    "Dec, 11, 2025": "{d}",
    "December 11": "{d}",
    "December 11 2025": "{d}",

    # extras
    "Dec 11 0000": "{d}",
    "December 31 9999": "{d}",
    "Dec  11,   2025": "{d}",
    "December 5, 25": "{d}",
    "Dec 11 2025 23:59": "{t}",
}

day_month_year_tests = {
    # originals
    "11 Dec 2025": "{d}",
    "11 Dec, 2025": "{d}",
    "11th Dec 2025": "{d}",
    "11th Dec, 2025": "{d}",

    # extras
    "01 Jan 0000": "{d}",
    "31 Dec 9999": "{d}",
    "1st Jan 25": "{d}",
    "1st Jan 25 14:00": "{t}",
    "11 Dec 2025 14:00 UTC": "{t}",
    "11 Dec 2025 14:00:59 PST": "{t}",
}

dow_date_tests = {
    # originals
    "Thu Dec 11, 2025": "{d}",
    "Thu, Dec 11, 2025": "{d}",
    "Mon Dec 1 2025": "{d}",
    "Tuesday, December 2 2025": "{d}",

    # extras
    "Wed 1/2/25": "{d}",
    "Fri 2025-12-11": "{d}",
    "fri 2025-12-11 14:00": "{t}",
    "Tuesday, 11 Dec 2025 14:00 - 15:30": "{t}",
}

ampm_tests = {
    # originals
    "Dec 11, 2025 2:30pm": "{t}",
    "Dec 11 2025 02:30 PM": "{t}",
    "11 Dec 2025 2:30pm": "{t}",
    "Thu Dec 11, 2025 2:30pm": "{t}",

    # extras
    "Dec 11, 2025 2pm": "{t}",
    "Dec 11, 2025 2 pm": "{t}",
    "11 Dec 2025 2 PM": "{t}",
    "11 Dec 2025 2:30 pm PST": "{t}",
}

time_range_tests = {
    # originals
    "Dec 11, 2025 2:30pm - 3:15pm": "{t}",
    "Thu Dec 11, 2025 2:45pm - 3:15pm (EST)": "{t}",
    "11 Dec 2025 14:00 - 15:30": "{t}",

    # extras
    "Dec 11 2025 2pm-3pm": "{t}",
    "11 Dec 2025 14:00 - 15:30 UTC": "{t}",
    "2025-12-11 14:00 - 15:30 (PST)": "{t}",
}

month_only_tests = {
    # originals
    "Dec": "{m}",
    "December": "{m}",
    "jul": "{m}",  # lower case, should still match
    "Meeting in October": "meeting in {m}",

    # extras
    "Sale ends in July": "sale ends in {m}",
    "See you in jan": "see you in {m}",
    "Billed through NOVEMBER": "billed through {m}",
}

id_tests = {
    # originals
    "Order #hsgske-heys": "order {#}",
    "Tracking ABC123": "tracking {#}",
    "Item A-1234 shipped Dec 11, 2025": "item {#} shipped {d}",

    # extras
    "Ref: INV-2025-12-11": "ref: {#}",
    "Ticket ID XZ-99-2025 opened on 11 Dec 2025":
        "ticket id {#} opened on {d}",
}

int_tests = {
    # originals
    "Invoice 12345": "invoice {i}",
    "Your code is 987": "your code is {i}",
    "Room 403": "room {i}",

    # extras
    "Balance: 0": "balance: {i}",
    "You have 10 messages": "you have {i} messages",
}

realistic_tests = {
    # originals
    "Appt confirmed: Thu Dec 11, 2025 2:45pm - 3:15pm (EST)":
        "appt confirmed: {t}",
    "Your appointment is scheduled for 04:30pm Mon, Dec 1, 2025":
        "your appointment is scheduled for {t}",
    "Delivery expected Dec, 24":
        "delivery expected {d}",
    "Package #abc-999 will arrive on December 5 2025":
        "package {#} will arrive on {d}",
    "Invoice 123 for order #hsgske-heys on 2025-12-03":
        "invoice {i} for order {#} on {d}",

    # extras
    "Order 123 placed on Dec 11, 2025 at 2:30pm":
        "order {i} placed on {t} at",
    "Reminder: Fri 1/2/25 9:00am - 10:00am (PST)":
        "reminder: {t}",
    "Billing statement for December 11 2025":
        "billing statement for {d}",
    "Your subscription renews in December":
        "your subscription renews in {m}",
    "Your code 987 expires on 2025-12-11":
        "your code {i} expires on {d}",
}


def run_tests(test_dict, label):
    local_dir = os.path.dirname(__file__)
    name_data = pickle.load(open(f"{local_dir}/name_automaton.pkl", "rb"))
    snorm = SubjectNormalizer(name_data)
    print(f"\n== {label} ==")
    for inp, expected in test_dict.items():
        out = snorm.normalize(inp)
        status = "OK " if out == expected else "ERR"
        print(f"{status} | {inp!r}")
        print(f"     expected: {expected!r}")
        print(f"     got:      {out!r}")


if __name__ == "__main__":
    # Run test suites
    for name, tests in [
        ("ISO Tests", iso_tests),
        ("Numeric Dates", numeric_date_tests),
        ("Month Day Year", month_day_year_tests),
        ("Day Month Year", day_month_year_tests),
        ("DOW + Date", dow_date_tests),
        ("AM/PM Tests", ampm_tests),
        ("Time Ranges", time_range_tests),
        ("Month Only", month_only_tests),
        ("ID Tests", id_tests),
        ("Integer Tests", int_tests),
        ("Realistic Mixed", realistic_tests),
    ]:
        run_tests(tests, name)

local_cache = os.path.dirname(__file__)

try:
    default_name_automaton = pickle.load(open(f"{local_cache}/name_automaton.pkl", "rb"))
except FileNotFoundError:
    default_name_automaton = None

# Default normalizer used by normalize_subject() to preserve old API.
default_normalizer = SubjectNormalizer(
    name_automaton=default_name_automaton,
    enable_names=(default_name_automaton is not None),
)