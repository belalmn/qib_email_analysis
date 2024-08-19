from datetime import datetime, timedelta, timezone
from email.parser import HeaderParser
from typing import Any, Dict, Optional

import pandas as pd
import pypff


def safe_getattr(obj, attr, default=None):
    try:
        value = getattr(obj, attr)
        if callable(value):
            return value()
        return value
    except Exception:
        return default


def parse_headers(message: pypff.message) -> Dict[str, Any]:
    headers = message.get_transport_headers()
    parsed_headers = HeaderParser().parsestr(headers)
    return {k.lower(): v for k, v in parsed_headers.items()} if parsed_headers else {}


def parse_timestamp(timestamp: Optional[int]) -> Optional[datetime]:
    return (
        datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=timestamp // 10)
        if timestamp
        else None
    )
