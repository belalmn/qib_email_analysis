import re
from collections import Counter
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
from hdbscan import HDBSCAN
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import normalize
from tqdm import tqdm
from wordcloud import WordCloud

from src.transform.llm_tools import invoke_llm

tqdm.pandas()


class TopicModellor:
    def __init__(
        self,
        message_df: pd.DataFrame,
        n_components_svd: int = 50,
        min_cluster_size: int = 10,
        min_samples: int = 5,
    ):
        self.n_components_svd = n_components_svd
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples

        topic_df = self.cluster_topics(message_df)[["message_id", "topic_id", "clean_text"]]
        topic_descriptions = self.get_topic_descriptions(topic_df)[["topic_id", "description"]]

        self.message_df = topic_df[["message_id", "topic_id"]].merge(message_df, on="message_id")
        self.topics_df = topic_df.merge(topic_descriptions, on="topic_id")[["topic_id", "description"]]
        self.word_frequencies = self.get_topic_word_frequencies(topic_df)[["topic_id", "word", "frequency"]]

    def cluster_topics(self, message_df: pd.DataFrame) -> pd.DataFrame:
        # Retrieve embeddings from embedding column in df
        embeddings = message_df["embeddings"].tolist()

        # Dimensionality reduction
        svd = TruncatedSVD(n_components=self.n_components_svd, random_state=42)
        reduced_embeddings = svd.fit_transform(embeddings)

        # Normalize the reduced embeddings
        normalized_embeddings = normalize(reduced_embeddings)

        # Clustering
        clusterer = HDBSCAN(min_cluster_size=self.min_cluster_size, min_samples=self.min_samples)
        cluster_labels = clusterer.fit_predict(normalized_embeddings)

        # Add results to dataframe
        message_df["topic_id"] = cluster_labels

        return message_df

    def get_topic_word_frequencies(self, message_df: pd.DataFrame) -> pd.DataFrame:
        topic_word_frequencies: Dict[str, Any] = {
            "unigrams": {},
            "bigrams": {},
            "trigrams": {},
        }

        # Loop through each unique topic_id
        for topic_id in tqdm(message_df["topic_id"].unique(), desc="Generating topic word frequencies"):
            topic_df = message_df[message_df["topic_id"] == topic_id]

            # Generate frequencies for unigrams, bigrams, and trigrams
            for ngram_type, n in zip(topic_word_frequencies.keys(), range(1, 4)):
                vectorizer = CountVectorizer(ngram_range=(n, n))
                bag_of_words = vectorizer.fit_transform(topic_df["clean_text"])
                sum_words = bag_of_words.sum(axis=0)
                words_freq = sorted(
                    [(word, sum_words[0, idx]) for word, idx in vectorizer.vocabulary_.items()],
                    key=lambda x: x[1],
                    reverse=True,
                )
                topic_word_frequencies[ngram_type][topic_id] = words_freq

        # Function to sample a certain number of rows
        def sample_top_n(words_freq, value):
            total = len(words_freq)
            return words_freq[:value]

        # Combine the frequencies into a single list of rows with proportional sampling
        rows = []
        for topic_id in message_df["topic_id"].unique():
            unigram_sample = sample_top_n(topic_word_frequencies["unigrams"][topic_id], 12)
            bigram_sample = sample_top_n(topic_word_frequencies["bigrams"][topic_id], 4)
            trigram_sample = sample_top_n(topic_word_frequencies["trigrams"][topic_id], 2)

            combined_samples = unigram_sample + bigram_sample + trigram_sample
            for word, freq in combined_samples:
                rows.append({"topic_id": topic_id, "word": word, "frequency": freq})

        # Create a single DataFrame
        combined_frequency_df = pd.DataFrame(rows)

        return combined_frequency_df

    # def generate_word_cloud(self, message_df: pd.DataFrame, topic_id: int) -> pd.DataFrame:
    #     topic_df = message_df[message_df["topic_id"] == topic_id]
    #     clean_text = " ".join(topic_df["clean_text"])
    #     wordcloud = WordCloud(width=800, height=400, background_color="white", min_font_size=10)
    #     wordcloud.generate(clean_text)
    #     return wordcloud

    def get_topic_descriptions(self, message_df: pd.DataFrame) -> pd.DataFrame:
        topic_descriptions = []
        topics_to_describe = (
            message_df[message_df["topic_id"] != -1].groupby("topic_id").filter(lambda x: len(x) >= 5)
        )
        for topic_id in topics_to_describe["topic_id"].unique():
            topic_df = message_df[message_df["topic_id"] == topic_id]
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
            description = invoke_llm(prompt).strip()
            description_df = pd.DataFrame({"topic_id": [topic_id], "description": [description]})
            topic_descriptions.append(description_df)
        return pd.concat(topic_descriptions, ignore_index=True)[["topic_id", "description"]]
