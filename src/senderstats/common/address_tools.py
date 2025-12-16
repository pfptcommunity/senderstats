import re

_ENTROPY_HEX_PAIRS_RE = re.compile(r'(?=(?:[0-9][a-f]|[a-f][0-9]|[0-9]{2}))', re.IGNORECASE)


def remove_prvs(email: str) -> str:
    """
    Removes PRVS tags from an email address for bounce attack prevention.

    :param email: The email address to clean.
    :return: The email address without PRVS tags.
    """

    if not email:
        return email

    at = email.find("@")
    if at < 0:
        return email

    # fast gate: avoid slicing in the common case
    if not (email.startswith("prvs") or email.startswith("msprvs")):
        return email

    local = email[:at]
    first = local.find("=")
    if first < 0:
        return email
    second = local.find("=", first + 1)
    if second < 0:
        return email

    return local[second + 1:] + email[at:]


def convert_srs(email: str) -> str:
    """
    Converts an email address from SRS back to its original form.

    :param email: The SRS modified email address.
    :return: The original email address before SRS modification.
    """

    if not email:
        return email

    at = email.find("@")
    if at < 0:
        return email

    p = email.find("srs")
    if p < 0 or p >= at:
        return email

    if p != 0 and email[p - 1] != "+":
        return email

    eq0 = email.find("=", p)
    if eq0 < 0 or eq0 >= at:
        return email

    eq1 = email.find("=", eq0 + 1)
    if eq1 < 0 or eq1 >= at:
        return email
    eq2 = email.find("=", eq1 + 1)
    if eq2 < 0 or eq2 >= at:
        return email
    eq3 = email.find("=", eq2 + 1)
    if eq3 < 0 or eq3 >= at:
        return email

    orig_domain = email[eq2 + 1: eq3]
    orig_local = email[eq3 + 1: at]
    if not orig_domain or not orig_local:
        return email

    return orig_local + "@" + orig_domain


def normalize_bounces(email: str) -> str:
    """
    Converts bounce addresses to a normal form removing the tracking data.

    :param email: The bounce modified email address.
    :return: The original email address or bounce modified email address.
    """

    if not email:
        return email

    at = email.find("@")
    if at <= 0:
        return email

    if email.startswith("bounces"):
        i = 7
        base = "bounces"
    elif email.startswith("bounce"):
        i = 6
        base = "bounce"
    else:
        return email

    if i >= at:
        return email
    c = email[i]
    if c != "+" and c != "-":
        return email

    return base + email[at:]


def normalize_entropy(email: str, entropy_threshold: float = 0.6, hex_pair_threshold: int = 6):
    """
        Determines if an email's local part suggests an automated sender based on entropy and hex pair count.

        Args:
            email (str): The full email address to analyze (e.g., "user@example.com").
            entropy_threshold (float): Minimum entropy score to consider the email automated (default: 0.6).
            hex_pair_threshold (int): Minimum number of hex pairs required to consider the email automated (default: 6).

        Returns:
            bool: True if the email is likely automated (high entropy and enough hex pairs), False otherwise.

        Note:
            Assumes `entropy_hex_pairs_re` is a pre-compiled regex (e.g., r'(?=(?:[0-9][a-fA-F]|[a-fA-F][0-9]|[0-9][0-9]))')
            defined globally to identify overlapping hex-like pairs.
    """
    try:
        local_part, domain_part = email.split("@")
    except ValueError:
        return email

    total_length = len(local_part)

    # Count character types
    numbers = sum(c.isdigit() for c in local_part)
    symbols = sum(c in "-+=_." for c in local_part)

    # Count hex pairs using regex
    hex_pairs = len([m.start() for m in _ENTROPY_HEX_PAIRS_RE.finditer(local_part)])

    # Weighted entropy
    weighted_entropy = (2 * hex_pairs + 1.5 * numbers + 1.5 * symbols) / total_length

    # Conditions
    is_high_entropy = weighted_entropy >= entropy_threshold
    has_enough_hex_pairs = hex_pairs >= hex_pair_threshold

    if is_high_entropy and has_enough_hex_pairs:
        return "#entropy#@" + domain_part

    return email
