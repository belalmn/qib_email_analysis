import datetime
import logging
import os

import pandas as pd


class DataFrameCheckpointer:
    """
    A utility class for saving and pulling dataframes to/from a checkpoint path.
    """

    def __init__(self, checkpoint_path: str) -> None:
        """
        Initializes a DataFrameCheckpointer with a given checkpoint path.

        Args:
        checkpoint_path (str): The path to the directory where dataframes should
        be saved or pulled from.
        """
        self.checkpoint_path = checkpoint_path

    def save(self, name: str, df: pd.DataFrame) -> None:
        """
        Saves a dataframe to the checkpoint path with a given name.

        Args:
        name (str): The name of the dataframe to save.
        df (pd.DataFrame): The dataframe to save.
        """
        df.to_csv(f"{self.checkpoint_path}/{name}.csv", index=False)
        logging.info(f"Saved {name} to checkpoint")

    def pull(self, name: str) -> pd.DataFrame:
        """
        Pulls a dataframe from the checkpoint path with a given name.

        Args:
        name (str): The name of the dataframe to pull.

        Returns:
        pd.DataFrame: The pulled dataframe, or None if no such dataframe exists.
        """
        logging.info(f"Pulling {name} from checkpoint")
        if os.path.exists(f"{self.checkpoint_path}/{name}.csv"):
            return pd.read_csv(f"{self.checkpoint_path}/{name}.csv")
        else:
            return None

    def pull_latest(self) -> pd.DataFrame:
        """
        Pulls the latest dataframe from the checkpoint path.

        Returns:
        pd.DataFrame: The pulled dataframe, or None if no such dataframe exists.
        """
        logging.info("Pulling latest from checkpoint")
        files = os.listdir(self.checkpoint_path)
        files = [self.checkpoint_path + "/" + f for f in files if f.endswith(".csv")]

        if len(files) == 0:
            return None
        else:
            latest_file = max(files, key=os.path.getctime)
            logging.info(f"Found latest file: {latest_file}")
            return pd.read_csv(f"{self.checkpoint_path}/{latest_file}")

