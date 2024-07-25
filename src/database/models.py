from datetime import datetime
from enum import Enum as PyEnum
from typing import List, Optional

from sqlalchemy import Enum, ForeignKey, String
from sqlalchemy.dialects.mysql import LONGTEXT, MEDIUMTEXT, TEXT, TINYTEXT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Folder(Base):
    __tablename__ = "folders"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True)


class Address(Base):
    __tablename__ = "addresses"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True)


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(primary_key=True)
    global_message_id: Mapped[str] = mapped_column(String(255), unique=True)
    folder_id: Mapped[int] = mapped_column(ForeignKey("folders.id"))
    from_address_id: Mapped[int] = mapped_column(ForeignKey("addresses.id"))
    provider_email_id: Mapped[int]
    creation_time: Mapped[datetime]
    submit_time: Mapped[datetime]
    delivery_time: Mapped[datetime]
    sender_name: Mapped[Optional[str]] = mapped_column(String(255, collation="utf8mb4_unicode_ci"))
    subject: Mapped[Optional[str]] = mapped_column(TEXT(64000, collation="utf8mb4_unicode_ci"))
    plain_text_body: Mapped[Optional[str]] = mapped_column(TEXT(64000, collation="utf8mb4_unicode_ci"))
    rich_text_body: Mapped[Optional[str]] = mapped_column(TEXT(64000, collation="utf8mb4_unicode_ci"))
    html_body: Mapped[Optional[str]] = mapped_column(TEXT(64000, collation="utf8mb4_unicode_ci"))
    first_in_thread: Mapped[Optional[bool]]
    previous_message_id: Mapped[Optional[str]] = mapped_column(String(255))
    domain: Mapped[Optional[str]] = mapped_column(String(255))
    language: Mapped[Optional[str]] = mapped_column(String(255))
    spam_score: Mapped[Optional[float]]
    from_internal_domain: Mapped[Optional[bool]]
    subject_prefix: Mapped[Optional[str]] = mapped_column(String(255))

    folder: Mapped["Folder"] = relationship()
    from_address: Mapped["Address"] = relationship()
    recipients: Mapped[List["Recipient"]] = relationship(back_populates="message")


class RecipientType(PyEnum):
    TO = "to"
    CC = "cc"
    BCC = "bcc"


class Recipient(Base):
    __tablename__ = "recipients"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.id"))
    address_id: Mapped[int] = mapped_column(ForeignKey("addresses.id"))
    type: Mapped[RecipientType] = mapped_column(Enum(RecipientType))

    message: Mapped["Message"] = relationship(back_populates="recipients")
    address: Mapped["Address"] = relationship()
