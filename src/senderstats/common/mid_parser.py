from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Dict, Any

#import pandas as pd

from senderstats.common.tld_parser import TLDParser


@dataclass(slots=True)
class MIDParserConfig:
    max_len: int = 512
    lowercase: bool = True
    keep_message_id: bool = True  # if False, returns only derived fields


class MIDParser:
    """
    Batch Message-ID parser (RFC-ish) that extracts RHS host token and splits it
    into infrastructure-friendly components using a TLDParser.

    Output columns (default):
      - Message_ID
      - mid_rfc
      - mid_host
      - mid_host_label
      - mid_subdomain
      - mid_domain
    """
    # --- RFC 5322 (almost allowing , in _ATEXT)---
    _ATEXT = r"[A-Za-z0-9!#$%&'*+\-/=?^_`{|}~,]"
    _DOT_ATOM = rf"{_ATEXT}+(?:\.{_ATEXT}+)*"
    _QTEXT = r"[\x21\x23-\x5B\x5D-\x7E]"
    _QPAIR = r"\\[\x00-\x09\x0B\x0C\x0E-\x7F]"
    _QUOTED_STR = rf"\"(?:{_QTEXT}|{_QPAIR})*\""
    _ID_LEFT = rf"(?:{_DOT_ATOM}|{_QUOTED_STR})"

    _DTEXT = r"[\x21-\x5A\x5E-\x7E]"
    _DLIT = rf"\[(?:{_DTEXT}|{_QPAIR})*\]"
    _ID_RIGHT = rf"(?:{_DOT_ATOM}|{_DLIT})"

    _ipv4_re = re.compile(
        r"^(?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}$"
    )
    _ipv6_re = re.compile(r"^[0-9a-f:]{2,}$", re.IGNORECASE)

    _msg_id_rfc_re = re.compile(rf"^\s*<?(?P<lhs>{_ID_LEFT})@(?P<rhs>{_ID_RIGHT})>?\s*$")

    __slots__ = ("tld", "cfg")

    def __init__(self, tld: TLDParser, config: Optional[MIDParserConfig] = None):
        self.tld = tld
        self.cfg = config or MIDParserConfig()

    # def parse_series(self, s: pd.Series) -> pd.DataFrame:
    #     """
    #     Parse a pandas Series of Message-ID values.
    #
    #     Returns a new DataFrame aligned to s.index.
    #     """
    #     # Ensure string dtype without copying more than needed
    #     s = s.astype("string")
    #
    #     # Prefilter to avoid regex cost on obvious non-candidates
    #     prefilter = s.str.contains("@", na=False) & (s.str.len() < self.cfg.max_len)
    #
    #     m = s.where(prefilter).str.extract(self._msg_id_rfc_re)
    #     rhs = m["rhs"]  # Series aligned to index
    #
    #     out = pd.DataFrame(index=s.index)
    #     out["mid_rfc"] = rhs.notna()
    #
    #     # Build mid_host ONCE (canonical RHS token for grouping/search)
    #     mid_host = rhs.astype("string").str.strip().str.rstrip(".")
    #     if self.cfg.lowercase:
    #         mid_host = mid_host.str.lower()
    #
    #     # Unbox domain-literals: [127.0.0.1] / [IPv6:...]
    #     is_lit = mid_host.notna() & mid_host.str.startswith("[") & mid_host.str.endswith("]")
    #     mid_host = mid_host.where(~is_lit, mid_host.str.slice(1, -1))
    #     mid_host = mid_host.where(~mid_host.str.startswith("ipv6:", na=False), mid_host.str.slice(5))
    #
    #     out["mid_host"] = mid_host
    #
    #     # Fast IP-ish detection (vectorized + gated)
    #     has_dot = mid_host.str.contains(".", regex=False, na=False)
    #     has_colon = mid_host.str.contains(":", regex=False, na=False)
    #
    #     is_ipv4 = has_dot & mid_host.str.match(self._ipv4_re)
    #     is_ipv6 = has_colon & mid_host.str.match(self._ipv6_re)
    #     is_ip = is_ipv4 | is_ipv6
    #
    #     # PSL split ONLY for dotted NON-IP hostnames
    #     mask_psl = mid_host.notna() & has_dot & ~is_ip
    #
    #     # Use TLDParser batch splitter for speed
    #     hosts = mid_host.loc[mask_psl].tolist()
    #     hl, sd, reg, _suf = self.tld.split_host_extended_parallel(hosts)
    #
    #     out.loc[mask_psl, "mid_host_label"] = hl
    #     out.loc[mask_psl, "mid_subdomain"] = sd
    #     out.loc[mask_psl, "mid_domain"] = reg
    #
    #     # Single-label hosts (non-IP, no dot): treat as host_label + domain = itself
    #     mask_single = mid_host.notna() & ~has_dot & ~is_ip
    #     out.loc[mask_single, "mid_host_label"] = out.loc[mask_single, "mid_host"]
    #     out.loc[mask_single, "mid_subdomain"] = ""
    #     out.loc[mask_single, "mid_domain"] = out.loc[mask_single, "mid_host"]
    #
    #     # IPs: host_label = full IP, domain = full IP
    #     out.loc[is_ip, "mid_host_label"] = out.loc[is_ip, "mid_host"]
    #     out.loc[is_ip, "mid_subdomain"] = ""
    #     out.loc[is_ip, "mid_domain"] = out.loc[is_ip, "mid_host"]
    #
    #     return out
    #
    # def parse_df(self, df: pd.DataFrame, col: str = "Message_ID") -> pd.DataFrame:
    #     """
    #     Parse Message-IDs from df[col]. Returns a new trimmed DataFrame.
    #
    #     If cfg.keep_message_id is True, includes the original Message_ID column.
    #     """
    #     parsed = self.parse_series(df[col])
    #     if self.cfg.keep_message_id:
    #         parsed.insert(0, "Message_ID", df[col].astype("string"))
    #     return parsed

    def parse(self, mid: Optional[str]) -> Dict[str, Any]:
        """
        Parse a single Message-ID value and return a dict with the same fields
        produced by parse_series().

        Keys:
          mid_rfc: bool
          mid_host: str
          mid_host_label: str
          mid_subdomain: str
          mid_domain: str
        """
        out: Dict[str, Any] = {
            "mid_rfc": False,
            "mid_host": "",
            "mid_host_label": "",
            "mid_subdomain": "",
            "mid_domain": "",
        }

        if mid is None:
            return out

        s = str(mid)

        # Prefilter (match parse_series semantics)
        if "@" not in s:
            return out
        if len(s) >= self.cfg.max_len:
            return out

        m = self._msg_id_rfc_re.match(s)
        if not m:
            return out

        rhs = m.group("rhs")
        out["mid_rfc"] = True

        # Canonical RHS token for grouping/search
        mid_host = (rhs or "").strip().rstrip(".")
        if self.cfg.lowercase:
            mid_host = mid_host.lower()

        # Unbox domain-literals: [127.0.0.1] / [IPv6:...]
        if mid_host.startswith("[") and mid_host.endswith("]"):
            mid_host = mid_host[1:-1]
        if mid_host.startswith("ipv6:"):
            mid_host = mid_host[5:]

        out["mid_host"] = mid_host

        # Fast IP-ish detection (gated)
        has_dot = "." in mid_host
        has_colon = ":" in mid_host

        is_ipv4 = has_dot and (self._ipv4_re.match(mid_host) is not None)
        is_ipv6 = has_colon and (self._ipv6_re.match(mid_host) is not None)
        is_ip = is_ipv4 or is_ipv6

        # PSL split ONLY for dotted NON-IP hostnames
        if mid_host and has_dot and not is_ip:
            host_label, subdomain, registrable, _suffix = self.tld.split_host_extended(mid_host)
            out["mid_host_label"] = host_label
            out["mid_subdomain"] = subdomain
            out["mid_domain"] = registrable
            return out

        # Single-label hosts (non-IP, no dot): host_label = itself, domain = itself
        if mid_host and (not has_dot) and (not is_ip):
            out["mid_host_label"] = mid_host
            return out

        # IPs: host_label = full IP, domain = full IP
        if mid_host and is_ip:
            out["mid_host_label"] = mid_host
            return out

        return out
