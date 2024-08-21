from typing import List

import numpy as np
import pandas as pd
import torch
import transformers
from tqdm import tqdm

tqdm.pandas()

products = [
    "Credit Card",
    "Family Shield",
    "Growing Deposit",
    "Misk Saving Account",
    "Personal Finance",
    "Supplementary Card",
    "Travel Takaful",
    "Credit Shield",
    "Auto Takaful",
    "Fixed Deposit",
    "Salary Transfer",
    "Saving Account",
    "Current Account",
    "Home Finance",
]

category_classifier = transformers.pipeline(
    task="zero-shot-classification",
    model="facebook/bart-large-mnli",
    device="cuda" if torch.cuda.is_available() else "cpu",
)

THRESHOLD = 0.09


def classify_products(df: pd.DataFrame) -> pd.DataFrame:
    def _classify_product(message: str) -> List[str]:
        result = category_classifier(message, candidate_labels=products, multi_label=True)
        labels = result["labels"]
        scores = result["scores"]
        labels = []
        for label, score in zip(labels, scores):
            if score > THRESHOLD:
                labels.append(label)
        return labels

    df["product"] = df["clean_text"].progress_apply(lambda x: _classify_product(str(x)))
    exploded_df = df.explode("product")
    exploded_df["product"] = exploded_df["product"].astype("category")
    return df[["message_id", "product"]]
