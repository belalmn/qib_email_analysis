import asyncio

import pandas as pd
import torch
import transformers

from src.transform.llm_invoker import LLMInvoker

category_classifier = transformers.pipeline(
    task="zero-shot-classification",
    model="facebook/bart-large-mnli",
    device="cuda" if torch.cuda.is_available() else "cpu",
)

THRESHOLD = 0.09

TEMPLATE = """
You are a multilingual spam detector. Classify the given text input as "spam" or "ham" categories. Please read each text carefully and determine its classification based on the definitions provided below.
<spam>: A spam message is an unsolicited and often repetitive communication sent to a large number of recipients, typically for the purpose of advertising products, services, or fraudulent schemes. It aims to promote content without the recipient's consent and can include unwanted links or malicious content.
<ham>:  A ham message refers to legitimate and desired communication that is relevant and expected by the recipient. These messages are typically personal, informational, or business-related, and are sent with the recipient's consent or in response to their interaction. 
Please read the following message and classify it as either "spam" or "ham" based on the provided definitions.
"""


def classify_spam_messages_with_llm(df: pd.DataFrame, llm_invoker: LLMInvoker) -> pd.DataFrame:
    df.loc[:, "prompt"] = df["clean_text"].apply(
        lambda x: f"{TEMPLATE}\n\nMessage: {x}\n\nClassification:"
    )
    result = llm_invoker.invoke_llms_df(df, "prompt")
    df.loc[:, "is_spam"] = result.apply(lambda x: "spam" in x and "ham" not in x)
    return df[["message_id", "is_spam"]]


def zero_shot_classify_spam_messages(df: pd.DataFrame) -> pd.DataFrame:
    def _classify_message(message: str) -> bool:
        result = category_classifier(message, candidate_labels=["spam", "ham"])
        return result["labels"][0] == "spam"

    df = df.copy()
    df["is_spam"] = df["clean_text"].progress_apply(lambda x: _classify_message(str(x)))
    return df[["message_id", "is_spam"]]
