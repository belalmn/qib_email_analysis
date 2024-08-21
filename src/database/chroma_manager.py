from typing import Any, Dict, List

import chromadb
import numpy as np
import pandas as pd
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings, IncludeEnum
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from sentence_transformers import SentenceTransformer
from tqdm.auto import tqdm


class SentenceEmbedding(EmbeddingFunction[Documents]):
    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self.model = SentenceTransformer(model_name)

    def __call__(self, input) -> Embeddings:
        # embed the documents somehow
        embeddings = []
        pool = self.model.start_multi_process_pool()
        docs = [str(doc) for doc in input]
        for embedding in tqdm(self.model.encode_multi_process(docs, pool=pool)):
            embeddings.append(embedding.tolist())
        return embeddings


class ChromaManager:
    def __init__(
        self, collection_name: str, path: str = "../../data/chroma", model_name: str = "all-MiniLM-L6-v2"
    ):
        settings = Settings()
        settings.allow_reset = True
        self.client = chromadb.PersistentClient(path=path, settings=settings)
        self.collection = self.client.get_or_create_collection(
            name=collection_name, embedding_function=SentenceEmbedding(model_name)
        )

    def add_documents_from_df(self, df: pd.DataFrame):
        documents = [str(doc) for doc in df["clean_text"].tolist()]
        self.collection.add(
            documents=documents,
            metadatas=[{"clean_text": clean_text} for clean_text in df["clean_text"].tolist()],
            ids=df["message_id"].tolist(),
        )

    def drop_collection(self) -> None:
        self.client.reset()

    def populate_embeddings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Populates the embeddings for the given dataframe,
        either by retrieving them from the database if they are already there,
        or by calculating them and adding them to the database.
        """
        ids = df["message_id"].tolist()

        # Retrieve the embeddings already in the database
        existing = self.collection.get(ids=ids)
        existing_ids = existing["ids"]
        print(f"Found {len(existing_ids)} embeddings in the database")

        # Add new documents to the database
        print(f"Adding {len(df) - len(existing_ids)} new documents to the database")
        df_without_embeddings = df.loc[~df["message_id"].isin(existing_ids)]
        if len(df_without_embeddings) > 0:
            self.add_documents_from_df(df_without_embeddings)
        print(f"Added {len(df_without_embeddings)} documents to the database")

        # Retrieve the embeddings for all the documents in the database
        all_embeddings = self.collection.get(ids=ids, include=[IncludeEnum.embeddings])

        # Create a dataframe mapping message_id to embedding
        embedding_df = pd.DataFrame(columns=["ids", "embeddings"])
        embedding_df["message_id"] = all_embeddings["ids"]
        embedding_df["embeddings"] = all_embeddings["embeddings"]

        # Join the embeddings with the original dataframe
        df_with_embeddings = pd.merge(df, embedding_df, on="message_id")
        return df_with_embeddings
