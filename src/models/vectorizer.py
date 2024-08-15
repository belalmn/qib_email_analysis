from typing import Optional, Tuple

import numpy as np
import pandas as pd
from scipy.sparse import spmatrix
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer

from src.database.chroma_db.chroma_manager import ChromaManager

model = SentenceTransformer("all-MiniLM-L6-v2")


class Vectorizer:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df
        self.chroma_manager: Optional[ChromaManager] = None

    def create_sentence_embeddings(self) -> np.ndarray:
        embeddings = model.encode(self.df["clean_text"].tolist())
        if not isinstance(embeddings, np.ndarray):
            raise ValueError("Unexpected return type from model.encode")
        return embeddings

    def append_to_chroma(self, collection_name: str, embeddings: np.ndarray) -> None:
        self.chroma_manager = ChromaManager(collection_name)
        self.chroma_manager.add_embeddings(self.df, embeddings)

    # def query_chroma(self, query_text: str, n_results: int = 5) -> pd.DataFrame:
    #     if self.chroma_manager is None:
    #         raise ValueError("ChromaManager has not been initialized. Call append_to_chroma() first.")

    #     results = self.chroma_manager.query(query_embedding=model.encode([query_text]), n_results=n_results)
    #     return results
