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
    if address and "@" in address:
        address = re.sub(r"\r\n\s*", " ", address).lower()
        addresses = getaddresses([address])
        valid_addresses = [addr for _, addr in addresses if addr.strip()]
        return valid_addresses[0] if len(valid_addresses) == 1 else ", ".join(valid_addresses)
    return None


def parse_identifiers(identifiers: Optional[str]) -> Optional[str]:
    if identifiers:
        matches = re.findall(r"<([^<>\s]+)>", identifiers)
        return ", ".join(matches)
    return None


def charset_from_content_type(content_type: Optional[str]) -> Optional[str]:
    if content_type:
        charset = re.search(r"charset\s*=\s*([^\s;]+)", content_type)
        return charset.group(1) if charset else None
    return None


def prefix_from_subject(subject: Optional[str]) -> Optional[str]:
    if subject:
        match = re.match(r"(re|fwd?|fw):\s*", subject.strip().lower())
        return match.group().strip() if match else None
    return None


def parse_body(body: Optional[bytes], encoding: Optional[str]) -> Optional[str]:
    if body:
        try:
            return body.decode(encoding) if encoding else body.decode()
        except UnicodeDecodeError:
            encoding = chardet.detect(body)["encoding"]
            return body.decode(encoding, errors="replace") if encoding else None
    return None


def parse_email_threading(df: pd.DataFrame) -> pd.DataFrame:
    df["first_in_thread"] = (
        df["previous_message_id"].isnull() & df["references"].isnull()
        if "references" in df.columns and "previous_message_id" in df.columns
        else None
    )
    df["num_previous_messages"] = df["references"].str.count(",") + 1
    df["thread_id"] = df["references"].apply(lambda x: x.split(",")[0] if x else None)
    return df


def parse_domain_info(df: pd.DataFrame) -> pd.DataFrame:
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
    df["is_internal"] = df["all_domains"].apply(
        lambda x: all("qib" in domain for domain in str(x).split(", "))
    )
    return df


def fill_plain_text_body(df: pd.DataFrame) -> pd.DataFrame:
    replace_df = df.loc[df["html_body"].notnull() & df["plain_text_body"].isnull()].copy()
    replace_df["plain_text_body"] = replace_df["html_body"].progress_apply(htmlConverter.handle)

    df.loc[df["html_body"].notnull() & df["plain_text_body"].isnull(), "plain_text_is_converted"] = True
    df.loc[df["html_body"].notnull() & df["plain_text_body"].isnull(), "plain_text_body"] = replace_df[
        "plain_text_body"
    ]

    df["plain_text_is_converted"] = df["plain_text_is_converted"].fillna(False)
    return df
