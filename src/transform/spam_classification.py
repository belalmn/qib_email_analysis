from typing import List

import pandas as pd

from src.transform.llm_tools import invoke_llm

TEMPLATE = """
You are a multilingual spam detector. Classify the given text input as "spam" or "ham" categories. Please read each text carefully and determine its classification based on the definitions provided below.
<spam>: A spam message is an unsolicited and often repetitive communication sent to a large number of recipients, typically for the purpose of advertising products, services, or fraudulent schemes. It aims to promote content without the recipient's consent and can include unwanted links or malicious content.
<ham>:  A ham message refers to legitimate and desired communication that is relevant and expected by the recipient. These messages are typically personal, informational, or business-related, and are sent with the recipient's consent or in response to their interaction. 
Please read the following message and classify it as either "spam" or "ham" based on the provided definitions.
"""


def classify_spam_messages(df: pd.DataFrame) -> pd.DataFrame:
    def _classify_message(message: str) -> bool:
        prompt = f"{TEMPLATE}\n\nMessage: {message}\n\nClassification:"
        result = invoke_llm(prompt)
        return "spam" in result and "ham" not in result

    df["is_spam"] = df["clean_text"].progress_apply(lambda x: _classify_message(str(x)))
    return df[["message_id", "is_spam"]]
