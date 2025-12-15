from __future__ import annotations

import pickle
from functools import lru_cache
from importlib import resources
from typing import Any

import re2
import regex as re

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

_RESOURCE_PACKAGE = "senderstats.common.data"
_RESOURCE_NAME_AUTOMATON = "name_automaton.pkl"


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

    @classmethod
    def load_default(cls) -> SubjectNormalizer:
        """
        Create a SubjectNormalizer using the packaged default name automaton (if present).
        Offline-safe: if the resource isn't shipped, it falls back to names disabled.
        """
        automaton: Optional[Any]
        try:
            ref = resources.files(_RESOURCE_PACKAGE).joinpath(_RESOURCE_NAME_AUTOMATON)
            with ref.open("rb") as f:
                automaton = pickle.load(f)
        except FileNotFoundError:
            automaton = None

        return cls(
            name_automaton=automaton,
            enable_names=(automaton is not None),
        )

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
        # s = self._replace_names(s)
        s = self._replace_months(s)
        s = re.sub(r"\s+", " ", s)
        return s.lower()


@lru_cache(maxsize=1)
def get_default_normalizer() -> SubjectNormalizer:
    return SubjectNormalizer.load_default()


def normalize_subject(subj: str) -> str:
    return get_default_normalizer().normalize(subj)
