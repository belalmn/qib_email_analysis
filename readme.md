# QIB Email Analytics Project

## Table of Contents
- [Project Description](#project-description)
- [Project Structure](#project-structure)
- [Pipeline Overview](#pipeline-overview)
  - [Extraction Workflow](#extraction-workflow)
  - [Transformation Workflow](#transformation-workflow)
  - [Loading Workflow](#loading-workflow)
- [LLM Usage in the Project](#llm-usage-in-the-project)
  - [LLM Invoker Module](#llm-invoker-module)
  - [LLM-Powered Features](#llm-powered-features)

## Project Description

The **Email Analysis ETL Pipeline** is a Python-based project developed for Qatar Islamic Bank (QIB). The bank manages over 5,000 emails per month through their public-facing email, `info@qib.com.qa`, but faces challenges in manually processing and analyzing these emails due to the high volume and limited resources. The project addresses these challenges by automating the extraction, transformation, and analysis of the emails, utilizing a combination of traditional data analysis techniques and cutting-edge large language models (LLMs).

#### Problem Statement

QIB receives thousands of emails monthly but is unable to efficiently track and extract valuable insights from these messages. Important information such as customer demographics, email intent, frequently discussed topics, response times, and potential spam content are unexplored due to manual limitations. This project solves the problem by building an automated ETL (Extract, Transform, Load) pipeline to process the `.pst` email files and extract actionable insights.

#### Project Goals

The main objectives of this project include:
- **Automated Email Processing**: Extract emails from `.pst` files and an optional IMAP server, allowing QIB to ingest and preprocess email data automatically.
- **LLM-Powered Email Analysis**: Implement large language models to derive insights such as email intent, topic modeling, sentiment analysis, and summarization.
- **Spam Detection**: Identify and filter out spam emails using both traditional and zero-shot classification methods.
- **Preprocessing and Feature Engineering**: Clean and prepare the email data for analysis, including cleaning text, calculating response times, detecting languages, and extracting important entities.
- **Data Storage**: Store the transformed email data and derived insights into a `MySQL` database, enabling further exploration and reporting.

#### Key Features

1. **PST File Parsing**: The pipeline ingests emails from `.pst` files, ensuring that the data is extracted efficiently, even from legacy formats.
2. **IMAP Integration**: Optional integration with QIB's IMAP server allows for real-time extraction of missing or additional emails from specific mailboxes.
3. **LLM-Enhanced Processing**: Cutting-edge language models are used to detect topics, classify email intent, summarize messages, and extract key information such as entities and sentiments.
4. **Spam Classification**: Combines traditional rule-based spam detection with zero-shot classification using large language models to accurately detect and filter spam.
5. **Feature Engineering**: Prepares email data for analysis by generating sentence embeddings, calculating response times, and detecting the language used in each message.
6. **Modular and Scalable**: The pipeline is designed to be modular, with distinct stages for extraction, transformation, and loading, allowing future extension and scalability.
7. **Database Integration**: Processed and transformed data is stored in a database for easy querying, further analysis, and report generation.

#### Technologies Used
- **Python**: Core language for the entire pipeline.
- **Pandas**: For data manipulation and processing.
- **ChromaDB**: For managing and retrieving sentence embeddings.
- **LLM**: Large language models from `Hugging Face` are used for topic modeling, spam detection, and summarization.
- **IMAP**: Integrated for fetching missing emails from the server.
- **MySQL**: Data is stored and managed in a relational `MySQL` database, providing long-term storage and query capabilities for the processed emails.

This project provides QIB with an automated and scalable solution for extracting and analyzing large volumes of emails, transforming raw data into actionable insights. The pipeline not only saves time and effort for the staff but also opens up new opportunities for strategic data-driven decision-making based on customer communication trends, while embracing modern AI methodologies for handling unstructured data.

## Project Structure

Below is an overview of the key components in the repository:

```
email_analysis 
├── config.json                         # Project-wide configurations for the pipeline
├── data/                               # Directory containing various stages of data
│   ├── checkpoints/                    # Intermediate data checkpoints
│   ├── chroma/                         # Chroma database files for embeddings
│   ├── interim/                        # Preprocessed and intermediate email data
│   ├── processed/                      # Final cleaned and processed email data
│   ├── raw/                            # Raw .pst email data
│   └── test/                           # Sample data used for testing and validation
├── requirements.txt                    # Python dependencies for the project
└── src/                                # Main source code for the email analysis pipeline
    ├── __init__.py                     # Package initialization file
    ├── config/                         # Configuration loading and management module
    │   ├── __init__.py
    │   └── config.py                   # Central configuration file for the pipeline
    ├── database/                       # Modules related to database management
    │   ├── __init__.py
    │   ├── chroma_manager.py           # Handles chroma database operations for embeddings
    │   ├── database.py                 # Handles database connections and queries
    │   ├── db_models.py                # Defines database models for storing data
    │   └── export_utils.py             # Utility functions for exporting database data
    ├── extract/                        # Modules responsible for email extraction
    │   ├── __init__.py
    │   ├── imap_extractor.py           # Module for extracting emails via IMAP
    │   ├── imap_parsing_utils.py       # Utility functions for parsing IMAP emails
    │   ├── parsing_utils.py            # General parsing utilities
    │   ├── pst_extractor.py            # Module for extracting emails from .pst files
    │   └── pst_parsing_utils.py        # Utility functions for parsing .pst emails
    ├── load/                           # Modules responsible for loading data
    │   ├── __init__.py
    │   └── data_loader.py              # Handles loading of cleaned data for further processing
    ├── models/                         # Pre-trained and fine-tuned models used for analysis
    │   ├── __init__.py
    │   ├── all-MiniLM-L6-v2/           # Pre-trained MiniLM model for sentence embeddings
    │   └── bart-large-mnli/            # Pre-trained BART model for NLI classification
    ├── notebooks/                      # Jupyter notebooks for data exploration and testing
    │   ├── __init__.py
    │   ├── extract.ipynb               # Notebook for data extraction workflows
    │   └── transform.ipynb             # Notebook for data transformation workflows
    ├── transform/                      # Modules responsible for data transformation
    │   ├── __init__.py
    │   ├── email_summary.py            # Summarizes key points from emails
    │   ├── llm_invoker.py              # Invokes LLM-based models for tasks like intent detection
    │   ├── message_classification.py   # Classifies email messages based on topics or intent
    │   ├── message_transformer.py      # Transforms raw messages for downstream processing
    │   ├── ner.py                      # Named entity recognition module
    │   ├── product_classification.py   # Classifies products from email content
    │   ├── spam_classification.py      # Detects spam messages
    │   └── topic_modelling.py          # Performs topic modeling on emails
    └── utils/                          # Utility modules used throughout the project
        ├── __init__.py
        └── checkpoint.py               # Utility functions for handling checkpoint data
```

### Directory Breakdown

`config.json`: Contains global configuration settings for the pipeline, such as paths, model settings, and environment variables.

`data/`: This folder contains all the datasets processed by the pipeline.

`checkpoints/`: Stores intermediate processing results like classified and cleaned messages.

`chroma/`: Contains Chroma database files that store embeddings and related data.

`interim/`: Preprocessed data from various stages, used before final transformation.

`processed/`: Final processed and clean data ready for analysis.

`raw/`: Raw data such as .pst email files collected directly from the source.

`test/`: Contains test datasets used for validation and experimentation.

`requirements.txt`: Lists all the Python dependencies required to run the project. These dependencies can be installed using pip.

`src/`: The main source directory that holds the core modules and scripts for running the pipeline.

`config/`: Handles configuration settings, loading configurations dynamically based on the environment.

`database/`: Manages all database operations, including Chroma database management, database models, and export utilities.

`extract/`: Responsible for extracting email data from different sources like .pst files and IMAP email servers.

`load/`: This module handles the loading of data into various formats, databases, or for further analysis.

`models/`: Contains pre-trained and fine-tuned models used in the analysis (e.g., for intent detection, topic modeling, and spam classification).

`notebooks/`: Jupyter notebooks that contains data pipeline workflows.

`transform/`: This module is responsible for transforming the raw email data, including summarization, classification, topic modeling, and invoking large language models (LLMs).

`utils/`: A set of utility functions used across the project, such as handling checkpoints.

## Pipeline Overview

### Extraction Workflow

1. **Imports and Configuration**:

    - The necessary modules and configurations are loaded at the beginning of the pipeline. Key components include:
        - `IMAPExtractor` and `PSTExtractor` from the `src.extract` module to handle email extraction from IMAP servers and PST files, respectively.
        - `DataFrameCheckpointer` from the `src.utils` module to manage checkpoints during the extraction process, ensuring that intermediate results are saved and retrievable.
        - Various utilities like `clean_text`, `get_response_time`, and `get_language` are imported from the `src.transform.message_transformer` module for later use in preprocessing and transformation steps.

2. **Ingesting from PST Files**:

    - `PSTExtractor` is used to extract email data from PST files. The PST files are located in a specified directory, and all files with the `.pst` extension are collected.
    - A `pst_message_df` dataframe is created, containing the extracted email data from the PST files.
    - This dataframe is then checkpointed to ensure that the raw extracted messages are saved (`ingested_messages.csv`).

3. **Handling Missing Emails**:

    - After ingesting from PST files, the pipeline checks for any missing emails (i.e., emails referenced in the system but not found in the PST files). These missing emails are identified and stored in the `missing_email_ids` list.
    - If missing emails are detected, the pipeline prepares to retrieve these emails from an IMAP server in the next step.

4. **Ingesting from IMAP Server (Optional Step)**:

    - If there are missing emails, the `IMAPExtractor` is used to connect to an IMAP server and extract the missing emails.
    - This step is optional and is only triggered when there are missing emails that could not be retrieved from PST files.
    - The emails are extracted from specified mailboxes on the server (e.g., INBOX, Sent Mail), and the resulting dataframe (`imap_message_df`) is merged with the previously extracted PST emails.

5. **Concatenating Messages**:

    - Once all available emails are extracted (both from PST and IMAP), the pipeline concatenates these messages into a single dataframe (`message_df`).
    - At this point, further preprocessing is performed to prepare the data for analysis.

6. **Preprocessing and Cleaning**:

    - **Text Cleaning**: The email content (body of the email) is cleaned using the `clean_text` function, which removes unwanted characters, HTML tags, and normalizes the text. The cleaned text is checkpointed as `clean_text_messages.csv`.
    - **Response Time Calculation**: Using the `get_response_time` function, the pipeline calculates the time it took to respond to each email (if applicable). This information is added to the dataframe and saved as `response_time_messages.csv`.
    - **Language Detection**: The `get_language` function is applied to detect the language of each email based on its cleaned text. The language is then saved in the `language_messages.csv` checkpoint.

7. **Sample and Export**:

    - A sample of 500 preprocessed messages is saved as `sample_preprocessed_messages.csv` for quick inspection and review.
    - The fully preprocessed dataset is then exported to `preprocessed_messages.csv` for use in the transformation stage of the pipeline.

### Transformation Workflow

1. **Imports and Configuration**:

    - Similar to the extraction phase, the pipeline starts by importing necessary modules and setting up the configuration for the transformation tasks.
    - The `LLMInvoker` is used to invoke large language models for various tasks, and `ChromaManager` is utilized for handling sentence embeddings.
    - The pipeline is set to load preprocessed data from checkpoints and work on filtered subsets of messages for various feature engineering tasks.

2. **Retrieving Preprocessed Messages**:

    - The transformation phase begins by loading the preprocessed messages from the checkpoint. This includes reading the full dataset of processed emails.

3. **Filtering Emails**:

    - Emails are filtered to focus on customer-oriented emails by excluding internal and outgoing emails. Specifically, emails sent internally (e.g., from `info@qib.com.qa`) are removed from the dataset, and duplicate messages are eliminated.

4. **Spam Classification**:

    - The pipeline filters out spam emails using zero-shot spam classification with a pre-trained language model.
    - Non-spam emails are saved as a new checkpoint (`spam_classified_messages`).

5. **Vectorization of Emails**:

    - Sentence embeddings are created using the `ChromaManager` and a pre-trained sentence transformer model. These embeddings are used to vectorize the content of the emails, enabling more advanced tasks like topic modeling and classification.

6. **Feature Engineering and Modeling**:

    - **Intent Analysis (Topic Modeling)**:
        - A topic model is applied to group emails into different topics based on their content. The resulting topics are further summarized and described using a language model.
        - Word frequencies for each topic are also calculated and stored for later analysis.

    - **Message Classification**:
        - Emails are classified into predefined categories to further segment the messages into relevant groups.
    - **Product Classification**:
        - The pipeline classifies products or services mentioned in the emails, which is important for identifying customer interests and queries.
    - **Named Entity Recognition (NER)**:
        - `NER` is applied to extract entities (such as names, locations, or organizations) from the email content.
    - **Email Summarization**:
        - Each email is summarized using a pre-trained LLM to extract key points from the messages.

7. **Final DataFrames**:

    - Separate list-like columns (e.g., addresses, references) are exploded into new dataframes, making it easier to load the data into structured tables.
        - **Address DataFrame**: Contains all email addresses involved in the email, broken down by sender and recipient types.
        - **Reference DataFrame**: Contains message references for each email, useful for tracking conversation threads.
        - **Domain DataFrame**: Lists the sender's domain for each email, providing insights into the source of messages.

8. **Exporting Data**:

    - After processing, the final transformed data is saved into multiple CSV files:
        - `messages_{DATE}.csv`
        - `addresses_{DATE}.csv`
        - `references_{DATE}.csv`
        - `domains_{DATE}.csv`
        - `word_frequencies_{DATE}.csv`
        - `topics_{DATE}.csv`
        - `classification_{DATE}.csv`
        - `products_{DATE}.csv`
        - `entities_{DATE}.csv`
        - `summaries_{DATE}.csv`
    - Each file contains a different aspect of the transformed data, ready for loading into a database.

### Loading Workflow

1. **Database Setup**:

    - The database is cleared and prepared to receive new data. Necessary tables are created if they don’t already exist.

2. **Loading DataFrames**:

    - Each transformed dataframe is loaded into the database using the `DataLoader`. Dataframes are processed to handle `NaN` values appropriately before being inserted into the respective database tables.
    - The following tables are populated:
        - `messages`: Contains all emails, including their content and metadata.
        - `addresses`: Holds the exploded list of email addresses involved in each message.
        - `references`: Tracks references between emails (useful for thread analysis).
        - `domains`: Lists the domains of email senders.
        - `word_frequencies`: Word frequency data for each topic.
        - `topics`: Contains the topics identified from the emails, along with descriptions.
        - `classifications`: Contains the results of email category classification.
        - `products`: Contains the classification of products or services mentioned in the emails.
        - `entities`: Contains named entities extracted from the emails.
        - `summaries`: Contains the summarized versions of emails.

### LLM Usage in the Project

The **Email Analysis ETL Pipeline** leverages large language models (LLMs) to enhance the accuracy and depth of several key features, such as Named Entity Recognition (NER), Email Summarization, and Topic Modelling. These models enable the pipeline to handle complex, unstructured email data in ways that traditional methods might not fully capture. The LLMs in this project are managed by the `LLMInvoker` module, which serves as the interface for invoking model-based inference across various tasks.

#### LLM Invoker Module

The `LLMInvoker` is the central component that integrates different LLMs (such as models from Hugging Face and Ollama). It provides functionality to invoke models for text generation and to apply LLMs across entire dataframes asynchronously. By handling prompt construction and inference with these models, `LLMInvoker` ensures the pipeline can use AI to enhance several important processing steps.

Key functionalities include:
- **Synchronous and Asynchronous Inference**: Allows the pipeline to perform inference on email data either one-by-one or across entire datasets concurrently.
- **Customizable Model Selection**: Supports switching between different LLMs based on project needs (e.g., Hugging Face models or Ollama models).

### LLM-Powered Features

1. **Named Entity Recognition (NER)**
   - The NER module detects important entities in emails, such as customer identification numbers, mobile numbers, account details, and transaction amounts. The `LLMInvoker` helps perform entity extraction by constructing prompts with a pre-defined template and passing these prompts to the model.
   - LLMs are used to extract entities when the email text is too complex or unstructured for regular expressions alone. The model generates a JSON-like response containing extracted entities, which is then parsed back into a structured format.

2. **Email Summarization**
   - This module summarizes emails written in either Arabic or English into concise, actionable summaries in English. The purpose of this feature is to condense lengthy or detailed emails into a brief overview covering the main points, key requests, and tone of the message.
   - The `LLMInvoker` generates these summaries by providing the email body as a prompt to the model, with guidance on what key elements to include in the summary.

3. **Topic Modelling**
   - The Topic Modelling module clusters emails into distinct topics, helping the pipeline understand common themes or inquiries. Once the clusters are formed, LLMs are used to generate short, descriptive labels for each topic based on sample emails within the cluster.
   - The `LLMInvoker` sends sample emails to the model and retrieves one-sentence topic descriptions, which help categorize and provide insight into the grouped messages.

By integrating LLMs, the pipeline enhances its ability to handle unstructured email data, extracting meaningful insights in a way that is both automated and scalable. This approach not only improves the potential for data-driven tasks like NER and topic modeling but also provides the flexibility to handle the natural language variations in customer communications.

