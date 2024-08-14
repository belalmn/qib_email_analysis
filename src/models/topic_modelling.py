from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfVectorizer
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
    
    def get_word_cloud_for_each_topic(self, n: int = 10) -> list[list[Tuple[str, int]]]:
        """
        Get the top words for each topic along with their frequency.
        """
        word_clouds = []
        for topic in range(self.df["lda_topic"].nunique()):
            topic_mask = self.df["lda_topic"] == topic
            topic_tfidf = self.tfidf_matrix[topic_mask]
            column_sums = np.asarray(topic_tfidf.sum(axis=0)).ravel()
            top_indices = column_sums.argsort()[-n:][::-1]
            top_words = [(self.tfidf.get_feature_names_out()[i], column_sums[i]) for i in top_indices]
            word_clouds.append(top_words)
        return word_clouds