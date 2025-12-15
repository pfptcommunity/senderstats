from typing import Dict, Tuple, Iterable, List
import re

PARSE_EMAIL_REGEX = r'(?:\s*"?([^"]*)"?\s)?(?:<?([a-zA-Z0-9!#$%&\'*+\/=?^_`{|}~\.-]+@[\[\]_a-zA-Z0-9\.-]+)>?)'

email_re = re.compile(PARSE_EMAIL_REGEX, re.IGNORECASE)

def parse_email_details_tuple(email_str: str) -> Tuple[str, str, str]:
    m = email_re.match(email_str or "")
    if not m:
        return "", "", ""
    display_name = m.group(1) or ""
    email_address = m.group(2) or ""
    _, _, domain = email_address.partition("@")
    return display_name, email_address, domain


def parse_email_details(email_str: str) -> Dict[str, str]:
    """
    Parses email details from a given email string.

    This function uses a regular expression to extract the display name and email address
    from a provided email string. If the email string matches the expected format, it extracts
    the display name (if present), the email address, and the domain part of the email address.
    If the email string does not match the expected format, it returns empty strings for these components.

    Parameters:
    :param email_str: The email string to parse. Expected to potentially include a display name
      and an email address in a standard format (e.g., "John Doe <john.doe@example.com>").

    :return: A dictionary containing the parsed display name, email address, and domain, as well as
      the original email string. The keys in the returned dictionary are "display_name", "email_address",
    """
    display_name, email_address, domain = parse_email_details_tuple(email_str)
    return {
        "display_name": display_name,
        "email_address": email_address,
        "domain": domain,
        "odata": email_str,
    }

def parse_email_details_parallel(emails: Iterable[str]) -> Tuple[List[str], List[str]]:
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
