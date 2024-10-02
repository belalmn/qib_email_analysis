from typing import List

import numpy as np
import pandas as pd
import torch
import transformers
from tqdm import tqdm

tqdm.pandas()

classes = [
    "customer_inquiry",
    "customer_complaint",
    "customer_feedback",
    "account_issue",
    "loan_application",
    "fraud_report",
    "technical_support",
    "product_information",
    "marketing_opt_out",
    "internal_communication",
    "partner_inquiry",
    "vendor_communication",
    "job_application",
    "press_inquiry",
    "regulatory_correspondence",
    "phishing_attempt",
    "spam",
    "service_request",
    "branch_information",
    "event_rsvp",
    "other",
]

category_classifier = transformers.pipeline(
    task="zero-shot-classification",
    model="facebook/bart-large-mnli",
    device="cuda" if torch.cuda.is_available() else "cpu",
)

THRESHOLD = 0.09


def classify_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify messages into predefined categories.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing the emails to classify.

    Returns
    -------
    pd.DataFrame
        Dataframe with the message_id and category columns.
    """
    def _classify_category(message: str) -> List[str]:
        result = category_classifier(message, candidate_labels=classes)
        labels = result["labels"]
        scores = result["scores"]
        labels = []
        for label, score in zip(labels, scores):
            if score > THRESHOLD:
                labels.append(label)
        return labels

    df["category"] = df["clean_text"].progress_apply(lambda x: _classify_category(str(x)))
    exploded_df = df.explode("category")
    exploded_df["category"] = exploded_df["category"].astype("category")
    return df[["message_id", "category"]]
