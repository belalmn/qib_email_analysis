from datetime import datetime, timedelta, timezone
from email.parser import HeaderParser
from typing import Any, Dict, Optional

import pandas as pd
import pypff


def safe_getattr(obj, attr, default=None):
    """
    A wrapper around getattr that provides a default value if the attribute
    does not exist or cannot be accessed due to an exception.

    :param obj: object to access the attribute from
    :param attr: attribute to access
    :param default: default value to return if an exception is raised
                   or if the attribute does not exist
    :return: attribute value or default value
    """
    try:
        value = getattr(obj, attr)
        if callable(value):
            return value()
        return value
    except Exception:
        return default


def parse_headers(message: pypff.message) -> Dict[str, Any]:
    """
    Parse email headers from a pypff.message object into a dictionary.

    The keys are lower-cased to ensure consistency.

    :param message: pypff.message object
    :return: dictionary of email headers
    """
    
    headers = message.get_transport_headers()
    parsed_headers = HeaderParser().parsestr(headers)
    return {k.lower(): v for k, v in parsed_headers.items()} if parsed_headers else {}


def parse_timestamp(timestamp: Optional[int]) -> Optional[datetime]:
    """
    Convert a PST timestamp to a datetime object.

    PST timestamps are represented as the number of 100-nanosecond intervals
    since January 1, 1601, in the UTC time zone. This function takes an
    optional PST timestamp and returns a datetime object in the UTC time
    zone representing the same time, or None if the input is None.

    :param timestamp: PST timestamp
    :return: datetime object in UTC time zone
    """
    return (
        datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=timestamp // 10)
        if timestamp
        else None
    )
