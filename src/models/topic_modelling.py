from typing import List, Tuple

import numpy as np
import pandas as pd
from hdbscan import HDBSCAN
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.decomposition import LatentDirichletAllocation, TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import normalize
from scipy.sparse import spmatrix

class TopicModellor:
    def __init__(
        self,
        df: pd.DataFrame,
        tfidf: TfidfVectorizer,
        tfidf_matrix: spmatrix,
        tfidf_embeddings: np.ndarray,
        n_components_lda: int = 20,
    ):
        self.df = df
        self.n_components_lda = n_components_lda
        self.tfidf = tfidf
        self.tfidf_matrix = tfidf_matrix
        self.tfidf_embeddings = tfidf_embeddings

    def perform_lda(self) -> pd.DataFrame:
        # Topic modeling with LDA
        lda = LatentDirichletAllocation(
            n_components=self.n_components_lda, random_state=42
        )
        lda_output = lda.fit_transform(self.tfidf_matrix)

        # Add results to dataframe
        self.df["lda_topic"] = lda_output.argmax(axis=1)

        return self.df