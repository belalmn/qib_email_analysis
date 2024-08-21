import logging
from os import path

from pydantic import BaseModel, ConfigDict, Field


class Config(BaseModel):
    model_config = ConfigDict(strict=True)

    # LLM
    use_ollama: bool = Field(default=False)
    llm_model_name: str = Field(default="microsoft/Phi-3-mini-4k-instruct")

    # Embeddings
    embedding_model_name: str = Field(default="all-MiniLM-L6-v2")

    # PST
    pst_directory: str = Field(default="../../data/raw/")
    output_directory: str = Field(default="../../data/processed/")

    # Database
    db_host: str = Field(default="localhost")
    db_user: str = Field(default="root")
    db_name: str = Field(default="email_analysis")
    db_password: str = Field(default="password")

    def normalize_paths(self):
        self.pst_directory = path.normcase(self.pst_directory)
        self.output_directory = path.normcase(self.output_directory)

    @classmethod
    def from_json(cls, config_path: str) -> "Config":
        try:
            with open(config_path, "r") as config_file:
                config_json: str = config_file.read()
                config = cls.model_validate_json(config_json)
                config.normalize_paths()
                return config
        except FileNotFoundError as e:
            logging.warning(f"Config file not found: {e}, using default config")
            config = cls()
            config.normalize_paths()
            return config
