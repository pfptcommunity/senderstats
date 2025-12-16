import regex as re

from senderstats.common.regex_patterns import *

# Precompiled Regex matches IPv4 and IPv6 addresses
ip_re = re.compile(IPV46_REGEX, re.IGNORECASE)

ooto_re = re.compile(
    r'(?i)(out[-\s]+of([-\s]+the)?[-\s]+office|auto(?:matic)?[-\s]*reply|autoreply|encrypted[-\s]message)')


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


def prepare_string_for_excel(text, max_length=32767):
    # Remove invalid XML control characters
    sanitized = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)

    sanitized = sanitized[:max_length]
    if len(sanitized) == max_length:
        # Problem \n is CR LF when stored by XML writer. This inserts 2 characters for ever \n exceeding the max length
        # Causing excel to warn the XLSX is malformed and ultimately truncates the data.
        count = sanitized.count('\n')
        if count > 1:
            sanitized = sanitized[:max_length - count]

    return sanitized


def is_ooo_or_autoreply(subject: str) -> bool:
    return bool(ooto_re.search(subject))
