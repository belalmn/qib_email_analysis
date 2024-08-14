import re
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
from langid.langid import LanguageIdentifier, model

identifier = LanguageIdentifier.from_modelstring(model, norm_probs=False)


def get_language(message: str) -> Optional[str]:
    language = identifier.classify(message)[0]
    if language in ["la", "qu"]:
        return "en"
    return identifier.classify(message)[0]


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
    parts = re.split(
        r"^-- \n.*|^--\n.*|^-----Original Message-----.*$|^________________________________.*$|^On .* wrote:.*|^From: .*|^Sent from my iPhone.*$",
        text,
        flags=re.MULTILINE | re.DOTALL,
        maxsplit=1,
    )
    
    text = parts[0].strip() if len(parts) > 1 else text
    text.replace("\n", " ").replace("\r", "").replace("\t", " ").replace("|", " ").replace("-", " ")

    # Remove any lines that are just whitespace
    text = "\n".join([line for line in text.split("\n") if line.strip()])

    return text
