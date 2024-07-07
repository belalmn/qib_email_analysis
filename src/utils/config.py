import logging
from os import path

from pydantic import BaseModel, ConfigDict, Field


class Config(BaseModel):
    model_config = ConfigDict(strict=True)
    input_pst_path: str = Field(default="./data/raw/emails.pst")
    output_directory: str = Field(default="./data/processed/")
    chunk_size: int = Field(default=250, ge=1)

    def normalize_paths(self):
        self.input_pst_path = path.normcase(self.input_pst_path)
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
