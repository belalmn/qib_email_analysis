from datetime import datetime
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Optional


def decode_str(string: Optional[str]) -> str:
    if not string:
        return ""
    decoded, encoding = decode_header(string)[0]
    if isinstance(decoded, bytes):
        return decoded.decode(encoding or "utf-8", errors="ignore")
    return str(decoded)


def parse_timestamp(email_date: Optional[str]) -> Optional[datetime]:
    if not email_date:
        return None
    return parsedate_to_datetime(email_date)
