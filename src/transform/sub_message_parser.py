import email.utils
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class SubMessage(BaseModel):
    subject: Optional[str]
    sender_name: Optional[str]
    from_address: Optional[str]
    submit_time: Optional[datetime]
    receiver_name: Optional[str]
    to_address: Optional[str]
    plain_body_text: str
    global_message_id: Optional[str]


def _parse_datetime(date_string: str) -> Optional[datetime]:
    parsed_date = email.utils.parsedate(date_string)
    if parsed_date:
        try:
            return datetime(*parsed_date[:6])
        except Exception as e:
            print(f"Error parsing date: {e}")
            return None
    else:
        return None


def _parse_email(email: str) -> Dict[str, Optional[str]]:
    search = re.search(r"(.*) <(.*)>", email)
    return (
        {"name": search.group(1), "email": search.group(2)} if search else {"name": None, "email": email}
    )


def _parse_sub_message(message: str, headers: str, type: str, reference) -> SubMessage:
    subject: Optional[str] = None
    sender_name: Optional[str] = None
    from_address: Optional[str] = None
    submit_time: Optional[datetime] = None
    receiver_name: Optional[str] = None
    to_address: Optional[str] = None
    plain_body_text: str = message
    global_message_id: str = reference

    # Extract header information
    if type == "from":
        """
        string looks like:
        From: John Doe <johndoe@gmail.com>
        Sent: Wednesday, January 3, 2018 5:00 PM
        To: Doe, Jane <janedoe@gmail.com>
        Subject: Re: Fwd: Send this to Bob
        """
        for header in headers.split("\n"):
            key, value = header.split(": ", 1)
            if key == "Subject":
                subject = value
            elif key == "From":
                sender_name, from_address = _parse_email(value).values()
            elif key == "Sent":
                submit_time = _parse_datetime(value)
            elif key == "To":
                receiver_name, to_address = _parse_email(value).values()

    elif type == "on":
        """
        string looks like:
        On Monday, Jan 1, 2018 at 12:00 PM, Jason
        <jason@gmail.com> wrote:
        """
        headers = headers.replace("\n", " ").replace("On ", "").replace(" at ", " ")
        last_comma = headers.rfind(",")
        if last_comma != -1:
            submit_time = _parse_datetime(headers[:last_comma])
            sender_name, from_address = _parse_email(headers[last_comma + 1]).values()

    sub_message = SubMessage(
        subject=subject,
        sender_name=sender_name,
        from_address=from_address,
        submit_time=submit_time,
        receiver_name=receiver_name,
        to_address=to_address,
        plain_body_text=plain_body_text,
        global_message_id=global_message_id,
    )

    return sub_message


def split_message(email_string: str, references: List[str]) -> List[SubMessage]:
    # Remove quoted text
    email_string = re.sub(r"^>+\s", "", email_string, flags=re.MULTILINE)
    pattern = r"(From:.*?$(?:\n[^:\n]+:[^\n]+$)*|On.*?at.*?,.*?\n?.*?wrote:)"

    # Split the email into messages
    splits = re.split(pattern, email_string, maxsplit=len(references), flags=re.MULTILINE | re.DOTALL)

    sub_messages: List[SubMessage] = []
    start_index = 0

    # Process first message (usually headerless)
    if not (splits[0].strip().startswith("On") or splits[0].strip().startswith("From")):
        start_index = 1
        sub_messages.append(
            SubMessage(
                subject=None,
                sender_name=None,
                from_address=None,
                submit_time=None,
                receiver_name=None,
                to_address=None,
                plain_body_text=splits[0],
                global_message_id=references[0],
            )
        )
    # Process subsequent messages
    for i in range(start_index, len(references) - start_index, 2):
        headers = splits[i]
        message = splits[i + 1]
        if headers.strip().startswith("On"):
            header_type = "on"
        elif headers.strip().startswith("From"):
            header_type = "from"
        else:
            header_type = "unknown"
        sub_messages.append(_parse_sub_message(message, headers, header_type, references[i]))

    # Process overflow messages (if any)
    if len(references) > len(sub_messages):
        sub_messages.append(
            SubMessage(
                subject=None,
                sender_name=None,
                from_address=None,
                submit_time=None,
                receiver_name=None,
                to_address=None,
                plain_body_text="".join(splits[-1:]),
                global_message_id=references[-1],
            )
        )

    return sub_messages
