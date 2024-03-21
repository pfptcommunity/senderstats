import email.utils
import re

from constants import PRVS_REGEX, SRS_REGEX, IPV46_REGEX

# Precompiled Regex for bounce attack prevention (PRVS) there prvs and msprvs1 (not much info on msprvs)
prvs_re = re.compile(PRVS_REGEX, re.IGNORECASE)

# Precompiled Regex for Sender Rewrite Scheme (SRS)
srs_re = re.compile(SRS_REGEX, re.IGNORECASE)

ip_re = re.compile(IPV46_REGEX, re.IGNORECASE)


def parse_email_details(email_str):
    display_name, email_address = email.utils.parseaddr(email_str)
    domain = email_address.split('@')[1] if '@' in email_address else ''
    return {"display_name": display_name, "email_address": email_address, "domain": domain, 'odata': email_str}


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


def get_message_id_host(msgid: str):
    """
    Extracts the host part from a message ID.

    The function assumes the message ID is in a typical format found in email headers,
    where it might be surrounded by '<' and '>' and contains an '@' symbol separating
    the local part from the host part. This function focuses on extracting the host part.

    Parameters:
    - msgid (str): The message ID from which the host part should be extracted.

    Returns:
    - str: The host part of the message ID. If the message ID does not contain an '@' symbol,
           the entire input string (minus any leading/trailing '<', '>', or spaces) is returned.

    Example:
    - Input: "<12345@example.com>"
    - Output: "example.com"
    """
    # Strip leading and trailing '<', '>', and spaces from the message ID,
    # then split it by the '@' symbol and return the last part (the host).
    # If '@' is not present, the entire stripped message ID is returned.
    return msgid.strip('<>[] ').split('@')[-1]
