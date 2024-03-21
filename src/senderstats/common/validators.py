import argparse
import re

from .constants import VALID_DOMAIN_REGEX, EMAIL_ADDRESS_REGEX


def is_valid_domain_syntax(domain_name: str):
    """
    Validates if the provided domain name follows the expected syntax.

    :param domain_name: Domain name to validate.
    :return: The domain name if valid.
    :raises: argparse.ArgumentTypeError if the domain name syntax is invalid.
    """
    if not re.match(VALID_DOMAIN_REGEX, domain_name, re.IGNORECASE):
        raise argparse.ArgumentTypeError(f"Invalid domain name syntax: {domain_name}")
    return domain_name


def is_valid_email_syntax(email: str):
    """
    Validates if the provided email follows the expected syntax.

    :param email: Email address to validate.
    :return: The email address if valid.
    :raises: argparse.ArgumentTypeError if the email syntax is invalid.
    """
    if not re.match(EMAIL_ADDRESS_REGEX, email, re.IGNORECASE):
        raise argparse.ArgumentTypeError(f"Invalid email address syntax: {email}")
    return email


def validate_xlsx_file(file_path):
    """
    Validates if the provided file path ends with a .xlsx extension.

    :param file_path: Path to the file to validate.
    :return: The file path if valid.
    :raises: argparse.ArgumentTypeError if the file does not end with .xlsx.
    """
    if not file_path.lower().endswith('.xlsx'):
        raise argparse.ArgumentTypeError("File must have a .xlsx extension.")
    return file_path
