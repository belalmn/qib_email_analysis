import logging
from datetime import datetime
from typing import List, Optional, Type, Union

import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from src.transform.message_enricher import EnrichedMessage

Base: Type = declarative_base()


class Folder(Base):
    __tablename__ = "folders"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)


class Address(Base):
    __tablename__ = "addresses"
    id = Column(Integer, primary_key=True)
    email = Column(String(255), nullable=False, unique=True)


class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True)
    global_message_id = Column(String(255), nullable=False, unique=True)
    folder_id = Column(Integer, ForeignKey("folders.id", name="fk_messages_folder_id"))
    from_address_id = Column(Integer, ForeignKey("addresses.id", name="fk_messages_from_address_id"))
    provider_email_id = Column(Integer, nullable=False)
    creation_time = Column(DateTime, nullable=False)
    submit_time = Column(DateTime, nullable=False)
    delivery_time = Column(DateTime, nullable=False)
    sender_name = Column(String(255, collation="utf8mb4_general_ci"))
    in_reply_to = Column(String(255))
    subject = Column(Text(collation="utf8mb4_general_ci"))
    body = Column(LONGTEXT(collation="utf8mb4_general_ci"))
    first_in_thread = Column(Boolean)
    previous_message_id = Column(String(255))
    domain = Column(String(255))
    language = Column(String(50))
    spam_score = Column(Float)
    from_internal_domain = Column(Boolean)
    subject_prefix = Column(String(255))

    folder = relationship("Folder")
    from_address = relationship("Address")
    recipients = relationship("Recipient")


class Recipient(Base):
    __tablename__ = "recipients"
    id = Column(Integer, primary_key=True)
    message_id = Column(Integer, ForeignKey("messages.id", name="fk_recipients_message_id"))
    address_id = Column(Integer, ForeignKey("addresses.id", name="fk_recipients_address_id"))
    type = Column(Enum("to", "cc", "bcc"), nullable=False)

    address = relationship("Address")


class DataLoader:
    def __init__(self, host: str, database: str, user: str, password: str):
        self.db_url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    def clear_tables(self):
        Base.metadata.drop_all(self.engine)
        logging.debug("Database tables cleared successfully")

    def create_tables(self):
        Base.metadata.create_all(self.engine)
        logging.debug("Database tables created successfully")

    def load(self, enriched_messages: List[EnrichedMessage]):
        session = self.Session()
        try:
            for message in enriched_messages:
                folder = self._get_or_create(session, Folder, name=message.folder_name)
                from_address = self._get_or_create(session, Address, email=message.from_address)

                db_message = Message(
                    provider_email_id=message.provider_email_id,
                    folder=folder,
                    from_address=from_address,
                    global_message_id=message.global_message_id,
                    creation_time=message.creation_time,
                    submit_time=message.submit_time,
                    delivery_time=message.delivery_time,
                    sender_name=message.sender_name,
                    in_reply_to=message.in_reply_to,
                    subject=message.subject,
                    body=message.body,
                    first_in_thread=message.first_in_thread,
                    previous_message_id=message.previous_message_id,
                    domain=message.domain,
                    language=message.language,
                    spam_score=message.spam_score,
                    from_internal_domain=message.from_internal_domain,
                    subject_prefix=message.subject_prefix,
                )
                session.add(db_message)

                self._add_recipients(session, db_message, message.to_address, "to")
                self._add_recipients(session, db_message, message.cc_address, "cc")
                self._add_recipients(session, db_message, message.bcc_address, "bcc")

            session.commit()
            logging.debug(f"Successfully loaded {len(enriched_messages)} messages into the database")
        except Exception as e:
            session.rollback()
            logging.error(f"Error while loading data into database: {e}")
        finally:
            session.close()

    def _get_or_create(self, session, model, **kwargs):
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            session.add(instance)
            session.flush()
            return instance

    def _add_recipients(self, session, db_message, addresses, recipient_type):
        if isinstance(addresses, list):
            for address in addresses:
                db_address = self._get_or_create(session, Address, email=address)
                db_message.recipients.append(Recipient(address=db_address, type=recipient_type))
        elif addresses:
            db_address = self._get_or_create(session, Address, email=addresses)
            db_message.recipients.append(Recipient(address=db_address, type=recipient_type))

    def export_to_csv(self, output_path: str):
        session = self.Session()
        try:
            tables = [Folder, Address, Message, Recipient]
            for table in tables:
                df = pd.read_sql(session.query(table).statement, session.bind)
                df.to_csv(f"{output_path}/{table.__tablename__}.csv", index=False, encoding="utf-8")
                logging.info(f"Exported {table.__tablename__} to CSV")
        except Exception as e:
            logging.error(f"Error while exporting data to CSV: {e}")
        finally:
            session.close()

    def export_to_excel(self, output_path: str):
        session = self.Session()
        try:
            tables = [Folder, Address, Message, Recipient]
            with pd.ExcelWriter(f"{output_path}/database_dump.xlsx") as writer:
                for table in tables:
                    df = pd.read_sql(session.query(table).statement, session.bind)
                    df.to_excel(writer, sheet_name=table.__tablename__, index=False, encoding="utf-8")
                    logging.debug(f"Added {table.__tablename__} to Excel File")
                logging.info("Exported database to Excel")
        except Exception as e:
            logging.error(f"Error while exporting data to Excel: {e}")
        finally:
            session.close()

    def close(self):
        self.engine.dispose()
        logging.debug("Database connection closed")
