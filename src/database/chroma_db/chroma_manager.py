import chromadb
import numpy as np
import pandas as pd
from chromadb.config import Settings


class ChromaManager:
    def __init__(self, collection_name: str, persist_directory: str = "./data/chroma"):
        self.client = chromadb.Client(
            Settings(chroma_db_impl="duckdb+parquet", persist_directory=persist_directory)
        )
        self.collection = self.client.get_or_create_collection(name=collection_name)

    def add_embeddings(self, df: pd.DataFrame, embeddings: np.ndarray):
        if len(df) != len(embeddings):
            raise ValueError("The number of rows in df must match the number of embeddings")

        ids = df["message_id"].astype(str).tolist()
        documents = df["clean_text"].tolist()
        metadata = df.to_dict("records")

        self.collection.add(
            ids=ids, embeddings=embeddings.tolist(), documents=documents, metadatas=metadata
        )

    def query(self, query_embedding: np.ndarray, n_results: int = 5):
        results = self.collection.query(query_embeddings=[query_embedding.tolist()], n_results=n_results)
        return results

    def get_collection_stats(self):
        return {"name": self.collection.name, "count": self.collection.count()}
