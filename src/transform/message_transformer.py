import re
from datetime import datetime
from typing import Dict, List, Optional, Union

# import nltk
import pandas as pd
from langid.langid import LanguageIdentifier, model
from tqdm import tqdm

# from nltk.corpus import stopwords
# from nltk.stem import PorterStemmer, WordNetLemmatizer

identifier = LanguageIdentifier.from_modelstring(model, norm_probs=False)

# nltk.download("punkt")
# nltk.download("punkt_tab")
# nltk.download("wordnet")
# nltk.download("stopwords")

tqdm.pandas()


def get_language(message: str) -> Optional[str]:
    """
    Identify the language of the given message.

    Args:
    message (str): The string message to identify the language from.

    Returns:
    Optional[str]: The identified language code or None if the language could not be identified.
    """
    language = identifier.classify(message)[0]
    if language in ["la", "qu"]:
        return "en"
    return identifier.classify(message)[0]


def get_response_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the response time for each message in the dataframe.

    This function takes a dataframe and adds a new column "response_time" which contains the time difference in seconds
    between the submit time of the current message and the submit time of the previous message of the same thread.

    Args:
    df (pd.DataFrame): Dataframe containing the messages.

    Returns:
    pd.DataFrame: Dataframe with the added "response_time" column.
    """
    df["response_time"] = None
    for index, row in tqdm(df.iterrows(), total=len(df)):
        previous_message_id = row["previous_message_id"]
        if previous_message_id:
            previous_row = df.loc[df["message_id"] == previous_message_id]
            if len(previous_row) > 0:
                previous_submit_time = datetime.fromisoformat(str(previous_row["submit_time"].iloc[0]))
                current_submit_time = datetime.fromisoformat(str(row["submit_time"]))
                response_time = (current_submit_time - previous_submit_time).total_seconds()
                df.at[index, "response_time"] = response_time
    return df


def clean_text(text: Optional[str]) -> str:
    """
    Clean the given text by removing any signature blocks and unnecessary whitespace.

    This function takes a string and removes any signature blocks, unnecessary whitespace, and caution notes
    that are commonly added to emails.

    Args:
    text (str): The string to clean.

    Returns:
    str: The cleaned string.
    """
    if not text or not isinstance(text, str):
        return ""
    parts = re.split(
        r"^-- \n.*|^--\n.*|^-----Original Message-----.*$|^________________________________.*$|^On .* wrote:.*|^From: .*|^Sent from my iPhone.*$",
        text,
        flags=re.MULTILINE | re.DOTALL,
        maxsplit=1,
    )

    text = parts[0].strip() if len(parts) > 1 else text
    text = re.sub(r"[\n\r\t| -]+", " ", text, flags=re.MULTILINE)
    caution_notes=[r"CAUTION: This email originated from outside QIB. Do not click any links or open attachments unless you are sure of the safety of the contents.",
                 r"""CAUTION: This email originated from outside of the organization. Do not click links or open attachments unless you recognize the sender and know the content is safe"""]
    for caution_note in caution_notes:
        text = re.sub(caution_note, "", text)

    # Remove any lines that are just whitespace
    text = "\n".join([line for line in text.split("\n") if line.strip()])

    return text