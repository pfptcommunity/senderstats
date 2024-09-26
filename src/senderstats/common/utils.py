import regex as re

from .regex_patterns import *

# Precompiled Regex for bounce attack prevention (PRVS) there prvs and msprvs1 (not much info on msprvs)
prvs_re = re.compile(PRVS_REGEX, re.IGNORECASE)

# Precompiled Regex for Sender Rewrite Scheme (SRS)
srs_re = re.compile(SRS_REGEX, re.IGNORECASE)

# Precompiled Regex matches IPv4 and IPv6 addresses
ip_re = re.compile(IPV46_REGEX, re.IGNORECASE)

# Precompiled Regex matches email addresses that can contain display names
email_re = re.compile(PARSE_EMAIL_REGEX, re.IGNORECASE)


def parse_email_details(email_str: str):
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
    # Attempt to match the email string against the predefined regex pattern
    match = email_re.match(email_str)

    # If the pattern matches, extract the display name and email address
    if match:
        display_name = match.group(1) or ''  # Extracted display name or empty string if not present
        email_address = match.group(2)  # Extracted email address
        # Extract the domain part from the email address if '@' is present, otherwise return empty string
        domain = email_address.split('@')[1] if '@' in email_address else ''
    else:
        # If the pattern does not match, initialize display name, email address, and domain as empty strings
        display_name, email_address, domain = '', '', ''

    # Return a dictionary containing the parsed components and the original email string
    return {
        "display_name": display_name,
        "email_address": email_address,
        "domain": domain,
        "odata": email_str
    }


def escape_regex_specials(literal_str: str):
    """
    Escapes regex special characters in a given string.

    :param literal_str: The string to escape.
    :return: A string with regex special characters escaped.
    """
    escape_chars = [".", "*", "+"]
    escaped_text = ""
    for char in literal_str:
        if char in escape_chars:
            escaped_text += "\\" + char
        else:
            escaped_text += char
    return escaped_text


def find_ip_in_text(data: str):
    """
    Find IPv4 or IPv6 address in a string of text

    :param data: string of information
    :return: String with IPv4 or IPv6 or empty if not found
    """
    match = ip_re.search(data)
    if match:
        return match.group()
    return ''


def build_or_regex_string(strings: list):
    """
    Creates a regex pattern that matches any one of the given strings.

    :param strings: A list of strings to include in the regex pattern.
    :return: A regex pattern string.
    """
    return r"({})".format('|'.join(strings))


def average(numbers: list) -> float:
    """
    Calculates the average of a list of numbers.

    :param numbers: A list of numbers.
    :return: The average of the numbers.
    """
    return sum(numbers) / len(numbers) if numbers else 0


def print_summary(title: str, data, detail: bool = False):
    """
    Prints a summary title followed by the sum of data values. If detail is True and data is a dictionary,
    detailed key-value pairs are printed as well. The function now also supports data being an integer,
    in which case it directly prints the data.

    :param title: The title of the summary.
    :param data: The data to summarize, can be an int, list, or dictionary.
    :param detail: Whether to print detailed entries of the data if it's a dictionary. This parameter
                   is ignored if data is not a dictionary.
    """
    if data is None:
        print(f"{title}: No data")
        return

    if isinstance(data, int):
        # Directly print the integer data
        print(f"{title}: {data}")
    elif isinstance(data, dict):
        # For dictionaries, sum the values and optionally print details
        data_sum = sum(data.values())
        print(f"{title}: {data_sum}")
        if detail:
            for key, value in data.items():
                print(f"  {key}: {value}")
            print()
    else:
        # Handle other iterable types (like list) by summing their contents
        try:
            data_sum = sum(data)
            print(f"{title}: {data_sum}")
        except TypeError:
            print(f"{title}: Data type not supported")


def remove_prvs(email: str):
    """
    Removes PRVS tags from an email address for bounce attack prevention.

    :param email: The email address to clean.
    :return: The email address without PRVS tags.
    """
    return prvs_re.sub('', email)


def convert_srs(email: str):
    """
    Converts an email address from SRS back to its original form.

    :param email: The SRS modified email address.
    :return: The original email address before SRS modification.
    """
    match = srs_re.search(email)
    if match:
        return '{}@{}'.format(match.group(3), match.group(2))
    return email


def compile_domains_pattern(domains: list) -> re.Pattern:
    """
    Compiles a regex pattern for matching given domains and subdomains, with special characters escaped.

    :param domains: A list of domain strings to be constrained.
    :return: A compiled regex object for matching the specified domains and subdomains.
    """
    # Escape special regex characters in each domain and convert to lowercase
    escaped_domains = [escape_regex_specials(domain.casefold()) for domain in domains]

    # Build the regex string to match these domains and subdomains
    regex_string = r'(\.|@)' + build_or_regex_string(escaped_domains)

    # Compile the regex string into a regex object
    pattern = re.compile(regex_string, flags=re.IGNORECASE)

    return pattern


def print_list_with_title(title: str, items: list):
    """
    Prints a list of items with a title.

    :param title: The title for the list.
    :param items: The list of items to print.
    """
    if items:
        print(title)
        for item in items:
            print(item)
        print()
