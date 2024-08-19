import logging
from typing import Dict, List, Type

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateTable

import src.database.db_models as db_models
from src.database.database import Database


class DataExporter:
    def __init__(self, database: Database):
        self.database = database
        self.models = self._get_models()

    # Get every class from db_models module that is a subclass of db_models.Base
    def _get_models(self) -> List[Type[db_models.Base]]:
        models = [
            getattr(db_models, model)
            for model in dir(db_models)
            if isinstance(getattr(db_models, model), type)
            and issubclass(getattr(db_models, model), db_models.Base)
            and getattr(db_models, model) != db_models.Base
        ]
        return models

    def export_to_csv(self, output_path: str) -> None:
        def _export(session: Session) -> None:
            for model in self.models:
                df = pd.read_sql(session.query(model).statement, session.bind)
                df.to_csv(f"{output_path}/{model.__tablename__}.csv", index=False, encoding="utf-8")
                logging.info(f"Exported {model.__tablename__} to CSV")

        self.database.execute_in_session(_export)

    def export_to_excel(self, output_path: str) -> None:
        def _export(session: Session) -> None:
            with pd.ExcelWriter(f"{output_path}/database_dump.xlsx", engine="xlsxwriter") as writer:
                for model in self.models:
                    df = pd.read_sql(session.query(model).statement, session.bind)
                    df.to_excel(writer, sheet_name=model.__tablename__, index=False)
                    logging.debug(f"Added {model.__tablename__} to Excel File")
                logging.info("Exported database to Excel")

        self.database.execute_in_session(_export)

    def to_dfs(self) -> Dict[str, pd.DataFrame]:
        def _to_dfs(session: Session) -> Dict[str, pd.DataFrame]:
            dfs: Dict[str, pd.DataFrame] = {}
            for model in self.models:
                df = pd.read_sql(session.query(model).statement, session.bind)
                df = df.set_index("id")
                dfs[model.__tablename__] = df
            return dfs

        return self.database.execute_in_session(_to_dfs)

    def export_schema(self, output_path: str) -> None:
        def _export_schema(session: Session) -> None:
            sql_statements = []
            for model in self.models:
                sql_statements.append(str(CreateTable(model.__table__).compile(self.database.engine)))

            with open(f"{output_path}/schema.sql", "w") as file:
                for statement in sql_statements:
                    file.write(f"{statement};\n\n")
                logging.info(f"Exported database schema to {output_path}/schema.sql")

        self.database.execute_in_session(_export_schema)
