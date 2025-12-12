import pandas as pd
from tldextract import tldextract

from senderstats.common.utils import parse_email_details, find_ip_in_text
from senderstats.interfaces.transform import Transform


class MIDTransform(Transform[pd.DataFrame, pd.DataFrame]):
    def __init__(self):
        super().__init__()

    @staticmethod
    def _extract_msgid_parts(msgid: str) -> tuple[str, str]:
        """
        From a single msgid string, return (msgid_host, msgid_domain).
        """
        if msgid is None or pd.isna(msgid):
            return "", ""

        msgid = str(msgid).strip()
        if not msgid:
            return "", ""

        parts = parse_email_details(msgid)

        # If parse_email_details didn't find an email-like address and there's no '@', bail
        if not (parts["email_address"] or "@" in msgid):
            return "", ""

        # Prefer parsed domain; fall back to simple split
        domain = parts["domain"] if parts["domain"] else msgid.split("@")[-1]

        # First: treat an IP in the domain specially
        host_ip = find_ip_in_text(domain)
        if host_ip:
            return host_ip, ""

        # Otherwise, use tldextract to break domain into pieces
        extracted = tldextract.extract(domain)
        # domain / suffix → example.com
        if extracted.suffix:
            msgid_domain = f"{extracted.domain}.{extracted.suffix}"
        else:
            msgid_domain = extracted.domain or ""

        msgid_host = extracted.subdomain

        # If there's no subdomain and no suffix (e.g., just "localhost" or similar),
        # we treat the whole thing as host, with empty domain.
        if not msgid_host and not extracted.suffix:
            msgid_host = msgid_domain
            msgid_domain = ""

        return msgid_host, msgid_domain

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        # Apply row-wise helper to msgid column
        parts = data["msgid"].apply(self._extract_msgid_parts)

        # Turn list/tuple of (host, domain) into two columns aligned with index
        data[["msgid_host", "msgid_domain"]] = pd.DataFrame(
            parts.tolist(),
            index=data.index,
            columns=["msgid_host", "msgid_domain"],
        )
        return data
