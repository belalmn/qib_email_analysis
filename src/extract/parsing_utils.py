import re
from email.utils import getaddresses
from typing import List, Optional

import chardet
import html2text
import pandas as pd
from tqdm import tqdm

htmlConverter = html2text.HTML2Text()
htmlConverter.ignore_links = True
htmlConverter.ignore_images = True
htmlConverter.ignore_emphasis = True

tqdm.pandas()


def parse_addresses(address: Optional[str]) -> Optional[str]:
    """
    Parse a string of email addresses and return a single string containing the valid
    addresses, or None if no valid addresses are found.

    Parameters
    ----------
    address : Optional[str]
        The string to parse, which may contain multiple email addresses separated
        by commas, semicolons, or whitespace.

    Returns
    -------
    Optional[str]
        A single string containing the valid addresses, or None if no valid addresses
        are found. If multiple valid addresses are found, they will be joined by
        commas.
    """
    if address and "@" in address:
        address = re.sub(r"\r\n\s*", " ", address).lower()
        addresses = getaddresses([address])
        valid_addresses = [addr for _, addr in addresses if addr.strip()]
        return valid_addresses[0] if len(valid_addresses) == 1 else ", ".join(valid_addresses)
    return None


def parse_identifiers(identifiers: Optional[str]) -> Optional[str]:
    """
    Extract valid email addresses from a string that may contain angle-bracketed
    identifiers.

    Parameters
    ----------
    identifiers : Optional[str]
        The string to parse, which may contain one or more angle-bracketed
        identifiers. For example, "<user@example.com>" or
        "<user@example.com> <another@example.com>".

    Returns
    -------
    Optional[str]
        A single string containing the valid identifiers, or None if no valid
        identifiers are found. If multiple valid identifiers are found, they will
        be joined by commas.
    """
    if identifiers:
        matches = re.findall(r"<([^<>\s]+)>", identifiers)
        return ", ".join(matches)
    return None


def charset_from_content_type(content_type: Optional[str]) -> Optional[str]:
    """
    Extract a charset from a content type string.

    Parameters
    ----------
    content_type : Optional[str]
        The content type string to parse, which may contain a charset
        declaration. For example, "text/plain; charset=utf-8".

    Returns
    -------
    Optional[str]
        The charset string, or None if no valid charset is found.
    """
    if content_type:
        charset = re.search(r"charset\s*=\s*([^\s;]+)", content_type)
        return charset.group(1) if charset else None
    return None


def prefix_from_subject(subject: Optional[str]) -> Optional[str]:
    """
    Extract a prefix from a subject string.

    Parameters
    ----------
    subject : Optional[str]
        The subject string to parse, which may contain a prefix
        like "re:", "fwd:", or "fw:"

    Returns
    -------
    Optional[str]
        The extracted prefix, or None if no valid prefix is found.
    """
    
    if subject:
        match = re.match(r"(re|fwd?|fw):\s*", subject.strip().lower())
        return match.group().strip() if match else None
    return None


def parse_body(body: Optional[bytes], encoding: Optional[str]) -> Optional[str]:
    """
    Decode a bytes object into a string, using the given encoding.

    If the encoding is not given, the bytes object is decoded using the
    default encoding. If decoding fails with a UnicodeDecodeError, the
    encoding is detected using chardet and the bytes object is decoded
    with the detected encoding. If decoding still fails, None is returned.

    Parameters
    ----------
    body : Optional[bytes]
        The bytes object to decode.
    encoding : Optional[str]
        The encoding to use for decoding.

    Returns
    -------
    Optional[str]
        The decoded string, or None if decoding fails.
    """
    if body:
        try:
            return body.decode(encoding) if encoding else body.decode()
        except UnicodeDecodeError:
            encoding = chardet.detect(body)["encoding"]
            return body.decode(encoding, errors="replace") if encoding else None
    return None


def parse_email_threading(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the threading information of a DataFrame of emails.

    The method infers the threading information of a series of emails from the
    "previous_message_id" and "references" fields. The "first_in_thread" column is
    set to True if the email is the first in a thread, i.e. if it is not a reply
    to any other email. The "num_previous_messages" column is set to the number of
    emails in the thread that occurred before this email. The "thread_id" column
    is set to the id of the first email in the thread.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame of emails.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the added threading information columns.
    """
    df["first_in_thread"] = (
        df["previous_message_id"].isnull() & df["references"].isnull()
        if "references" in df.columns and "previous_message_id" in df.columns
        else None
    )
    df["num_previous_messages"] = df["references"].str.count(",") + 1
    df["thread_id"] = df["references"].apply(lambda x: x.split(",")[0] if x else None)
    return df


def parse_domain_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse domain information from a DataFrame of emails.

    The method infers the sender domain and all domains mentioned in the email
    from the "from_address", "to_address", "cc_address", and "bcc_address"
    fields. It also infers whether the email is internal or external based on
    whether the sender domain is "qib" or not.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame of emails.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the added domain information columns.
    """
    def get_domain(address: Optional[str]) -> str:
        if address and "@" in address:
            return address.strip().split("@")[1]
        return ""

    def get_domains(addresses: Optional[str]) -> List[str]:
        if addresses:
            return [get_domain(address) for address in addresses.split(",")]
        return []

    def extract_unique_domains(row):
        domains = set(get_domains(row["from_address"]))

        for field in ["to_address", "cc_address", "bcc_address"]:
            domains.update(get_domains(row[field]))

        return ", ".join(sorted(domains))

    df["sender_domain"] = df["from_address"].apply(get_domain)
    df["all_domains"] = df.apply(extract_unique_domains, axis=1)
    df["is_internal"] = df["sender_domain"].apply(
        lambda x: "qib" in x #any("qib" in domain for domain in str(x).split(", "))
    )
    return df


def fill_plain_text_body(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill the plain_text_body column with converted HTML text if it is null. A boolean
    column named plain_text_is_converted is also added to indicate which rows were
    converted.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame of emails.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the filled plain_text_body column and the new
        plain_text_is_converted column.
    """
    replace_df = df.loc[df["html_body"].notnull() & df["plain_text_body"].isnull()].copy()
    replace_df["plain_text_body"] = replace_df["html_body"].progress_apply(htmlConverter.handle)

    df.loc[df["html_body"].notnull() & df["plain_text_body"].isnull(), "plain_text_is_converted"] = True
    df.loc[df["html_body"].notnull() & df["plain_text_body"].isnull(), "plain_text_body"] = replace_df[
        "plain_text_body"
    ]

    df["plain_text_is_converted"] = df["plain_text_is_converted"].fillna(False)
    return df
