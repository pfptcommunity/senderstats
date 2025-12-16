import re
from typing import Dict, Tuple, Iterable, List

# Keeping old just in case
# _PARSE_EMAIL_REGEX = r'(?:\s*"?([^"]*)"?\s)?(?:<?([a-zA-Z0-9!#$%&\'*+\/=?^_`{|}~\.-]+@[\[\]_a-zA-Z0-9\.-]+)>?)'
_PARSE_EMAIL_REGEX = r'^\s*(?:"(.*)"|(.*?))\s*(?:<?([A-Za-z0-9!#$%&\'*+\/=?^_`{|}~.-]+@[\[\]_A-Za-z0-9.-]+)>?)\s*$'

_EMAIL_RE = re.compile(_PARSE_EMAIL_REGEX, re.IGNORECASE)


def parse_email_details_tuple(email_str: str) -> Tuple[str, str, str]:
    """
    Parses an email string into its core components.

    Attempts to extract a display name, full email address, and domain
    from the provided string using a compiled regular expression.
    If parsing fails, empty strings are returned for all fields.

    :param email_str: An email string that may include an optional display name
                      and angle-bracketed email address.
    :return: A tuple of (display_name, email_address, domain).
    """
    m = _EMAIL_RE.match(email_str or "")
    if not m:
        return "", "", ""
    display_name = (m.group(1) or m.group(2) or "").strip() or ""
    email_address = m.group(3) or ""
    _, _, domain = email_address.partition("@")
    return display_name, email_address, domain


def parse_email_details(email_str: str) -> Dict[str, str]:
    """
    Parses email details from a given email string.

    This function extracts the display name, email address, and domain
    from an input string and returns them in a dictionary along with
    the original input value.

    :param email_str: The email string to parse. This may include a display name
                      and an email address in a standard format.
    :return: A dictionary with keys: display_name, email_address, domain, and odata.
    """
    display_name, email_address, domain = parse_email_details_tuple(email_str)
    return {
        "display_name": display_name,
        "email_address": email_address,
        "domain": domain,
        "odata": email_str,
    }


def parse_email_details_parallel(emails: Iterable[str]) -> Tuple[List[str], List[str]]:
    """
    Parses multiple email strings in sequence and collects results in parallel lists.

    For each input string, the display name and email address are extracted
    and appended to separate lists, preserving input order.

    :param emails: An iterable of email strings to parse.
    :return: A tuple containing two lists:
             (list_of_display_names, list_of_email_addresses).
    """
    display_names: List[str] = []
    email_addresses: List[str] = []

    core = parse_email_details_tuple
    dn_append = display_names.append
    ea_append = email_addresses.append

    for s in emails:
        dn, ea, _ = core(s)
        dn_append(dn)
        ea_append(ea)

    return display_names, email_addresses
