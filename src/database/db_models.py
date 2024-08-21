from datetime import datetime
from enum import Enum as PyEnum
from typing import List, Optional

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.dialects.mysql import LONGTEXT, TEXT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class RecipientType(PyEnum):
    FROM = "from"
    TO = "to"
    CC = "cc"
    BCC = "bcc"


# Updated Message Model
class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[str] = mapped_column(String(255), unique=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.topic_id"))
    is_spam: Mapped[Optional[bool]]
    subject: Mapped[Optional[str]] = mapped_column(TEXT(collation="utf8mb4_unicode_ci"))
    subject_prefix: Mapped[Optional[str]] = mapped_column(String(255))
    submit_time: Mapped[datetime]
    delivery_time: Mapped[datetime]
    html_body: Mapped[Optional[str]] = mapped_column(LONGTEXT(collation="utf8mb4_unicode_ci"))
    plain_text_body: Mapped[Optional[str]] = mapped_column(LONGTEXT(collation="utf8mb4_unicode_ci"))
    from_name: Mapped[Optional[str]] = mapped_column(String(255, collation="utf8mb4_unicode_ci"))
    previous_message_id: Mapped[Optional[str]] = mapped_column(String(255))
    first_in_thread: Mapped[Optional[bool]]
    num_previous_messages: Mapped[Optional[int]]
    thread_id: Mapped[Optional[str]] = mapped_column(String(255))
    sender_domain: Mapped[Optional[str]] = mapped_column(String(255))
    is_internal: Mapped[Optional[bool]]
    clean_text: Mapped[Optional[str]] = mapped_column(LONGTEXT(collation="utf8mb4_unicode_ci"))
    response_time: Mapped[Optional[int]]
    language: Mapped[Optional[str]] = mapped_column(String(255))

    # Relationships
    addresses: Mapped["Address"] = relationship(back_populates="message")
    references: Mapped["Reference"] = relationship(back_populates="message")
    domains: Mapped["Domain"] = relationship(back_populates="message")
    classifications: Mapped["Classification"] = relationship(back_populates="message")
    entities: Mapped["Entity"] = relationship(back_populates="message")
    summary: Mapped["Summary"] = relationship(back_populates="message")
    topic: Mapped["Topic"] = relationship(back_populates="messages")


class Address(Base):
    __tablename__ = "addresses"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.message_id"))
    address_type: Mapped[str] = mapped_column(Enum(RecipientType))
    address: Mapped[str] = mapped_column(String(255))

    # Relationship
    message: Mapped["Message"] = relationship(back_populates="addresses")


class Reference(Base):
    __tablename__ = "references"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.message_id"))
    reference_message_id: Mapped[str] = mapped_column(String(255))

    # Relationship
    message: Mapped["Message"] = relationship(back_populates="references")


class Domain(Base):
    __tablename__ = "domains"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.message_id"))
    domain: Mapped[str] = mapped_column(String(255))

    # Relationship
    message: Mapped["Message"] = relationship(back_populates="domains")


class WordFrequency(Base):
    __tablename__ = "word_frequencies"

    id: Mapped[int] = mapped_column(primary_key=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.topic_id"))
    word: Mapped[str] = mapped_column(String(255))
    frequency: Mapped[int]

    # Relationship
    topic: Mapped["Topic"] = relationship(back_populates="word_frequencies")


class Topic(Base):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(primary_key=True)
    topic_id: Mapped[str] = mapped_column(String(255), unique=True)
    topic_description: Mapped[Optional[str]] = mapped_column(TEXT(collation="utf8mb4_unicode_ci"))

    # Relationships
    messages: Mapped["Message"] = relationship(back_populates="topic")
    word_frequencies: Mapped["WordFrequency"] = relationship(back_populates="topic")


class Classification(Base):
    __tablename__ = "classifications"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.message_id"))
    category: Mapped[str] = mapped_column(String(255))

    # Relationship
    message: Mapped["Message"] = relationship(back_populates="classifications")


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.message_id"))
    product: Mapped[str] = mapped_column(String(255))

    # Relationship
    message: Mapped["Message"] = relationship(back_populates="products")


class Entity(Base):
    __tablename__ = "entities"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.message_id"))
    entity_type: Mapped[str] = mapped_column(String(255))
    entity_value: Mapped[str] = mapped_column(String(255))

    # Relationship
    message: Mapped["Message"] = relationship(back_populates="entities")


class Summary(Base):
    __tablename__ = "summaries"

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.message_id"))
    summary: Mapped[str] = mapped_column(LONGTEXT(collation="utf8mb4_unicode_ci"))

    # Relationship
    message: Mapped["Message"] = relationship(back_populates="summary")
