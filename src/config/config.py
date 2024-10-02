import logging
from os import path

from pydantic import BaseModel, ConfigDict, Field


class Config(BaseModel):
    """
    A class to store the configuration for the email analysis pipeline.

    Attributes:
        use_ollama (bool): Whether to use Ollama or not.
        llm_model_name (str): The name of the LLM model to use.
        embedding_model_name (str): The name of the embedding model to use.
        pst_directory (str): The path to the PST directory.
        output_directory (str): The path to the output directory.
        db_host (str): The hostname of the database.
        db_user (str): The username of the database.
        db_name (str): The name of the database.
        db_password (str): The password of the database.
    """

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
        """
        Reads a config file and returns a config object.

        Args:
            config_path (str): The path to the config file.

        Returns:
            Config: The config object.

        Raises:
            FileNotFoundError: If the config file is not found, a default config is used.
        """
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
