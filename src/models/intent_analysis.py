from typing import List, Tuple

import numpy as np
import pandas as pd
from hdbscan import HDBSCAN
from scipy.sparse import spmatrix
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.decomposition import LatentDirichletAllocation, TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import normalize


class IntentAnalyzer:
    def __init__(
        self,
        df: pd.DataFrame,
        tfidf: TfidfVectorizer,
        tfidf_matrix: spmatrix,
        n_components_svd: int = 50,
        min_cluster_size: int = 10,
        min_samples: int = 5,
    ):
        self.df = df
        self.n_components_svd = n_components_svd
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples
        self.tfidf = tfidf
        self.tfidf_matrix = tfidf_matrix

    def perform_clustering(self, embeddings: np.ndarray) -> pd.DataFrame:
        # Dimensionality reduction
        svd = TruncatedSVD(n_components=self.n_components_svd, random_state=42)
        reduced_embeddings = svd.fit_transform(embeddings)

        # Normalize the reduced embeddings
        normalized_embeddings = normalize(reduced_embeddings)

        # Clustering
        clusterer = HDBSCAN(min_cluster_size=self.min_cluster_size, min_samples=self.min_samples)
        cluster_labels = clusterer.fit_predict(normalized_embeddings)

        # Add results to dataframe
        self.df["cluster"] = cluster_labels

        return self.df

    def get_top_keywords(self, cluster: int, n: int = 10) -> List[Tuple[str, int]]:
        if self.tfidf is None:
            raise ValueError("Tfidf vectorizer not initialized")
        cluster_docs = self.df[self.df["cluster"] == cluster]
        tfidf_subset = self.tfidf.transform(cluster_docs["clean_text"])
        tfidf_sum = np.asarray(tfidf_subset.sum(axis=0)).ravel()
        top_indices = tfidf_sum.argsort()[-n:][::-1]
        top_words = [(self.tfidf.get_feature_names_out()[i], tfidf_sum[i]) for i in top_indices]
        return top_words

    def get_top_keywords_for_each_cluster(self) -> List[List[Tuple[str, int]]]:
        if self.tfidf is None:
            raise ValueError("Tfidf vectorizer not initialized")
        top_keywords = []
        for cluster in range(self.df["cluster"].nunique()):
            top_keywords.append(self.get_top_keywords(cluster))
        return top_keywords
