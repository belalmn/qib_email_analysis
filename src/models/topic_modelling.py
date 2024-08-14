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
    
    def get_top_words(self, n: int = 10) -> List[str]:
        """
        Get the top words for each topic.
        """
        top_indices = self.tfidf_matrix.sum(axis=0).argsort()[-n:][::-1]
        return [self.tfidf.get_feature_names_out()[i] for i in top_indices]
    
    def get_top_words_for_each_topic(self) -> list[list[str]]:
        """
        Get the top words for each topic.
        """
        top_words = []
        for topic in range(self.df["lda_topic"].nunique()):
            top_words.append(self.get_top_words(topic))
        return top_words