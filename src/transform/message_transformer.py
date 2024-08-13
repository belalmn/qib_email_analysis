import re
from datetime import datetime
from typing import Dict, List, Optional, Union

import langid
import pandas as pd


def get_language(message: str) -> Optional[str]:
    return langid.classify(message)[0]


def get_response_time(df: pd.DataFrame) -> pd.DataFrame:
    df["response_time"] = None
    for index, row in df.iterrows():
        previous_message_id = row["previous_message_id"]
        if previous_message_id:
            previous_row = df.loc[df["message_id"] == previous_message_id]
            if len(previous_row) > 0:
                previous_submit_time = datetime.fromisoformat(previous_row["submit_time"].iloc[0])
                current_submit_time = datetime.fromisoformat(row["submit_time"])
                response_time = (current_submit_time - previous_submit_time).total_seconds()
                df.at[index, "response_time"] = response_time
    return df


def clean_text(text: Optional[str]) -> str:
    if not text or not isinstance(text, str):
        return ""
    text = text.replace("\n", " ")
    text = re.sub(r"^[>].*?$\n", "", text, flags=re.MULTILINE)
    return text
