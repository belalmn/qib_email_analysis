import re
from collections import Counter

import numpy as np
import pandas as pd
from hdbscan import HDBSCAN
from langchain_community.llms.ollama import Ollama
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import normalize
from wordcloud import WordCloud
from typing import Dict, List, Tuple

llm = Ollama(model="llama2:7b")


class TopicModellor:
    def __init__(
        self,
        df: pd.DataFrame,
        n_components_svd: int = 50,
        min_cluster_size: int = 10,
        min_samples: int = 5,
    ):
        self.df = df
        self.n_components_svd = n_components_svd
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples

    def cluster_topics(self, embeddings: np.ndarray) -> pd.DataFrame:
        # Dimensionality reduction
        svd = TruncatedSVD(n_components=self.n_components_svd, random_state=42)
        reduced_embeddings = svd.fit_transform(embeddings)

        # Normalize the reduced embeddings
        normalized_embeddings = normalize(reduced_embeddings)

        # Clustering
        clusterer = HDBSCAN(min_cluster_size=self.min_cluster_size, min_samples=self.min_samples)
        cluster_labels = clusterer.fit_predict(normalized_embeddings)

        # Add results to dataframe
        self.df["topic_id"] = cluster_labels

        return self.df
    
    # def get_word_frequencies(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    #     """Get the word frequencies for each topic in the dataframe."""
    #     topic_word_frequencies = {
    #         "unigrams": {},
    #         "bigrams": {},
    #         "trigrams": {},
    #     }
        
    #     for topic_id in self.df["topic_id"].unique():
    #         topic_df = self.df[self.df["topic_id"] == topic_id]
            
    #         for ngram_type, n in zip(topic_word_frequencies.keys(), range(1, 4)):
    #             vectorizer = CountVectorizer(ngram_range=(n, n))
    #             bag_of_words = vectorizer.fit_transform(topic_df["clean_text"])
    #             sum_words = bag_of_words.sum(axis=0)
    #             words_freq = sorted(
    #                 [(word, sum_words[0, idx]) for word, idx in vectorizer.vocabulary_.items()],
    #                 key=lambda x: x[1],
    #                 reverse=True
    #             )
    #             topic_word_frequencies[ngram_type][topic_id] = words_freq
        
    #     # Create dataframes
    #     def create_df(freq_dict):
    #         rows = []
    #         for topic_id, word_freq_list in freq_dict.items():
    #             for word, freq in word_freq_list:
    #                 rows.append({"topic_id": topic_id, "word": word, "frequency": freq})
    #         return pd.DataFrame(rows)
        
    #     unigram_frequency_df = create_df(topic_word_frequencies["unigrams"])
    #     bigram_frequency_df = create_df(topic_word_frequencies["bigrams"])
    #     trigram_frequency_df = create_df(topic_word_frequencies["trigrams"])
        
    #     return unigram_frequency_df, bigram_frequency_df, trigram_frequency_df

    def get_word_frequencies(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        unigram_frequencies: Dict[str, List[Tuple[str, int]]] = {}
        bigram_frequencies: Dict[str, List[Tuple[str, int]]] = {}
        trigram_frequencies: Dict[str, List[Tuple[str, int]]] = {}
        
        for topic_id in self.df["topic_id"].unique():
            # Retrieve corpus for current topic
            topic_df = self.df[self.df["topic_id"] == topic_id]
            
            # Get unigram, bigram and trigram frequencies for current topic
            for n in range(1, 4):
                vec = CountVectorizer(ngram_range=(n,n)).fit(topic_df["clean_text"])
                bag_of_words = vec.transform(topic_df["clean_text"])
                sum_words = bag_of_words.sum(axis=0)
                words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
                words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True)
                
                if n == 1:
                    unigram_frequencies[topic_id] = words_freq
                elif n == 2:
                    bigram_frequencies[topic_id] = words_freq
                elif n == 3:
                    trigram_frequencies[topic_id] = words_freq

        # Create dataframes
        def create_df(freq_dict):
            rows = []
            for topic_id, word_freq_list in freq_dict.items():
                for word, freq in word_freq_list:
                    rows.append({"topic_id": topic_id, "word": word, "frequency": freq})
            return pd.DataFrame(rows)

        unigram_frequency_df = create_df(unigram_frequencies)
        bigram_frequency_df = create_df(bigram_frequencies)
        trigram_frequency_df = create_df(trigram_frequencies)

        return unigram_frequency_df, bigram_frequency_df, trigram_frequency_df

    def generate_word_cloud(self, topic_id: int) -> pd.DataFrame:
        topic_df = self.df[self.df["topic_id"] == topic_id]
        clean_text = " ".join(topic_df["clean_text"])
        wordcloud = WordCloud(width=800, height=400, background_color="white", min_font_size=10)
        wordcloud.generate(clean_text)
        return wordcloud

    def get_topic_descriptions(self) -> pd.DataFrame:
        topic_descriptions = []
        topics_to_describe = (
            self.df[self.df["topic"] != -1].groupby("topic").filter(lambda x: len(x) >= 5)
        )
        for topic_id in topics_to_describe["topic"].unique():
            topic_df = self.df[self.df["topic"] == topic_id]
            sample_messages = topic_df.sample(n=5)["clean_text"].tolist()
            prompt = f"""
            You will be given a set of sample emails that belong to the same topic. Your task is to describe the overall topic of these emails in one short and concise sentence.

            Important Guidelines:

            Do not include any greetings, explanations, or additional comments.
            Do not engage in conversation or provide a detailed breakdown.
            Only provide the final output as a one-sentence label for the topic.

            Examples:

            Example 1:

            Email Samples:

            Sample 1: "Hello, I am having trouble logging into my account. Could you help me reset my password?"
            Sample 2: "I forgot my password and can't access my account. Please assist with a password reset."
            Sample 3: "I need to reset my password, but I'm not receiving the reset email."
            Correct Description:
            Account login and password reset issues.

            Incorrect Description:
            These emails are about users having trouble with logging into their accounts and resetting their passwords. The emails mention forgotten passwords, issues with receiving reset emails, and a request for help to regain account access.

            Example 2:

            Email Samples:

            Sample 1: "I have a question about the pricing of your premium plan."
            Sample 2: "Could you explain the cost differences between your basic and premium subscription?"
            Sample 3: "I'm interested in upgrading to your premium service. What are the additional costs involved?"
            Correct Description:
            Inquiries about pricing and subscription plans.

            Incorrect Description:
            The topic of these emails is centered around pricing inquiries and subscription plans. The users are asking about the costs of premium services, the differences between the basic and premium plans, and how to upgrade their subscriptions.

            Your Turn:

            Email Samples:

            Sample 1: "{sample_messages[0]}"
            Sample 2: "{sample_messages[1]}"
            Sample 3: "{sample_messages[2]}"

            Description:
            """
            description = llm.invoke(prompt).strip()
            description_df = pd.DataFrame({"topic": [topic_id], "description": [description]})
            topic_descriptions.append(description_df)
        return pd.concat(topic_descriptions, ignore_index=True)
