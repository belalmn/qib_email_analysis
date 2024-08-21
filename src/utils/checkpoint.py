import datetime
import logging
import os

import pandas as pd


class DataFrameCheckpointer:
    def __init__(self, checkpoint_path: str) -> None:
        self.checkpoint_path = checkpoint_path

    def save(self, name: str, df: pd.DataFrame) -> None:
        df.to_csv(f"{self.checkpoint_path}/{name}.csv", index=False)
        logging.info(f"Saved {name} to checkpoint")

    def pull(self, name: str) -> pd.DataFrame:
        logging.info(f"Pulling {name} from checkpoint")
        if os.path.exists(f"{self.checkpoint_path}/{name}.csv"):
            return pd.read_csv(f"{self.checkpoint_path}/{name}.csv")
        else:
            return None

    def pull_latest(self) -> pd.DataFrame:
        logging.info("Pulling latest from checkpoint")
        files = os.listdir(self.checkpoint_path)
        files = [self.checkpoint_path + "/" + f for f in files if f.endswith(".csv")]

        if len(files) == 0:
            return None
        else:
            latest_file = max(files, key=os.path.getctime)
            logging.info(f"Found latest file: {latest_file}")
            return pd.read_csv(f"{self.checkpoint_path}/{latest_file}")
