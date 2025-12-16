from __future__ import annotations

from senderstats.common.address_parser import parse_email_details_tuple
from senderstats.common.tld_parser import TLDParser


class MIDParser:
    """
    Message-ID host parser for mail-gateway telemetry.

    Extracts RHS host from Message-ID-like values using the address parser,
    normalizes it, and splits it using the Public Suffix List.
    """

    __slots__ = ("tld",)

    def __init__(self, tld: TLDParser):
        self.tld = tld

    @staticmethod
    def is_ipv4_fast(s: str) -> bool:
        # Strict dotted-quad: A.B.C.D, each 0..255, 1-3 digits
        n = len(s)
        if n < 7 or n > 15:  # 0.0.0.0 .. 255.255.255.255
            return False

        dots = 0
        octet = 0
        digits = 0

        for ch in s:
            o = ord(ch)
            if 48 <= o <= 57:  # '0'..'9'
                digits += 1
                if digits > 3:
                    return False
                octet = octet * 10 + (o - 48)
                if octet > 255:
                    return False
            elif ch == '.':
                # need at least one digit before dot
                if digits == 0:
                    return False
                dots += 1
                if dots > 3:
                    return False
                octet = 0
                digits = 0
            else:
                return False

        return dots == 3 and digits != 0

    def parse(self, mid: str) -> tuple[str, str, str, str]:
        _, _, domain = parse_email_details_tuple(mid)
        if not domain:
            return "", "", "", ""

        if ':' in domain or '[' in domain or ']' in domain or '.' not in domain or self.is_ipv4_fast(domain):
            return domain, "", "", ""

        if domain[-1] == '.':
            domain = domain.rstrip('.')

        hn, sub, registrable, public_suffix = self.tld.split_host_extended(domain)
        return hn, sub, registrable, public_suffix
