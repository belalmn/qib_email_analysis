import logging
from typing import List, Type

import pandas as pd
from sqlalchemy.orm import Session

from src.database.database import Database
from src.database.models import Address, Base, Folder, Message, Recipient


class DataExporter:
    def __init__(self, database: Database):
        self.database = database

    def export_to_csv(self, output_path: str) -> None:
        def _export(session: Session) -> None:
            tables: List[Type[Base]] = [Folder, Address, Message, Recipient]
            for table in tables:
                df = pd.read_sql(session.query(table).statement, session.bind)
                df.to_csv(f"{output_path}/{table.__tablename__}.csv", index=False, encoding="utf-8")
                logging.info(f"Exported {table.__tablename__} to CSV")

        self.database.execute_in_session(_export)

    def export_to_excel(self, output_path: str) -> None:
        def _export(session: Session) -> None:
            tables: List[Type[Base]] = [Folder, Address, Message, Recipient]
            with pd.ExcelWriter(f"{output_path}/database_dump.xlsx", engine="xlsxwriter") as writer:
                for table in tables:
                    df = pd.read_sql(session.query(table).statement, session.bind)
                    df.to_excel(writer, sheet_name=table.__tablename__, index=False)
                    logging.debug(f"Added {table.__tablename__} to Excel File")
                logging.info("Exported database to Excel")

        self.database.execute_in_session(_export)

    def get_db_as_df(self) -> pd.DataFrame:
        def _get_df(session: Session) -> pd.DataFrame:
            query = (
                session.query(Message, Folder, Address, Recipient)
                .join(Folder)
                .join(Address)
                .outerjoin(Recipient)
            )
            return pd.read_sql(query.statement, session.bind)

        return self.database.execute_in_session(_get_df)
