import asyncio
import json
import re
from typing import Dict, List, Tuple, Union, Optional

import pandas as pd

from src.transform.llm_invoker import LLMInvoker

# Define regular expressions for different entities
ENTITY_PATTERNS = {
    "QID": r"\b\d{11}\b",  # 11-digit Qatar ID
    "Mobile Number": r"\+?\d{1,4}[\s-]?\d{1,4}[\s-]?\d{6,10}",  # International or local numbers
    "Account Number": r"\b\d{8,12}\b",  # General account number pattern with 8-12 digits
    "Passport Number": r"\b[A-Z0-9]{5,9}\b",  # Simplistic passport number pattern
    "Email Address": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",  # Email pattern
    "IBAN": r"\b[A-Z]{2}\d{2}[A-Z0-9]{1,30}\b",  # IBAN
    "Transaction Amount": r"\b[\$|QAR|USD|EUR]?\s?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\b",  # Currency with amount
    "Transaction Date": r"\b\d{4}-\d{2}-\d{2}\b",  # Date in YYYY-MM-DD format
    "Date of Birth": r"\b\d{4}-\d{2}-\d{2}\b",  # Same as Transaction Date for DOB
}

# Define the template for the prompt
TEMPLATE = """
Your task is to extract specific entities from emails sent to a bank (info@qib.com.qa). Please identify and extract the following entities if present:

- QID (11-Digit Qatar ID Number)
- Mobile Number
- Account Number
- Passport Number
- Full Name
- Email Address
- Date of Birth
- Transaction Amount
- Transaction Date
- IBAN (International Bank Account Number)

For each email, provide the extracted entities in JSON format. Sometimes, multiple entities for one category are present, include them in a list. The email may be in a different language, you are still expected to extract the entities that are present. If an entity is not present in the email, do not include it in the JSON output.

Examples:

Input:
Subject: Account Balance Inquiry

Dear Sir/Madam,

I would like to inquire about the balance of my account number 1234567890. My customer ID is 15153182861.

Thank you,
John Doe

Output:
{
  "Customer ID": "QID987654",
  "Account Number": "1234567890",
  "Full Name": "John Doe",
  "Email Address": "john.doe@example.com"
}

Input:
Subject: International Transfer

Hello,

I'd like to make an international transfer of $5000 and QAR300 on 2023-05-15. My account details are:

Name: Jane Smith
IBAN: GB29NWBK60161331926819
Mobile: +44 7911 123456

Best regards,
Jane

Output:
{
  "Full Name": "Jane Smith",
  "Mobile Number": "+447911123456",
  "Email Address": "jane.smith@example.com",
  "IBAN": "GB29NWBK60161331926819",
  "Transaction Amount": [
	"$5000",
	"QAR300"
  ]
  "Transaction Date": "2023-05-15"
}

Now, please extract entities from the following email.

Input:
"""


def extract_entities_using_regex(text: str) -> List[Tuple[str, str]]:
    """
    Extracts entities from the text using predefined regex patterns.

    Args:
        text (str): The text to extract entities from.

    Returns:
        List[Tuple[str, str]]: A list of tuples containing entity type and extracted value.
    """
    entities = []
    for entity_type, pattern in ENTITY_PATTERNS.items():
        matches = re.findall(pattern, text)
        if matches:
            for match in matches:
                entities.append((entity_type, match))
    return entities


def extract_entities_from_messages(df: pd.DataFrame, llm_invoker: LLMInvoker, use_regex: bool = False) -> pd.DataFrame:
    """
    Extracts entities from a dataframe of email messages using either regex or an LLM.

    Args:
        df (pd.DataFrame): The dataframe containing the email messages.
        llm_invoker (LLMInvoker): The LLMInvoker instance used for LLM-based entity extraction.
        use_regex (bool): Whether to use regex-based extraction or LLM-based extraction. Defaults to False.

    Returns:
        pd.DataFrame: A dataframe with extracted entities.
    """
    def _extract_entities_from_json(llm_response: str) -> List[Tuple[str, str]]:
        """
        Parses LLM response JSON and extracts entities.

        Args:
            llm_response (str): The LLM response in JSON format.

        Returns:
            List[Tuple[str, str]]: A list of entity type and value pairs.
        """
        entities = []
        try:
            output = json.loads(llm_response)
        except json.JSONDecodeError:
            output = []
        if isinstance(output, dict):
            for entity_type, entity_value in output.items():
                if isinstance(entity_value, str):
                    entities.append((entity_type, entity_value))
                elif isinstance(entity_value, list):
                    for e in entity_value:
                        entities.append((entity_type, e))
        return entities

    if use_regex:
        df["entities"] = df["clean_text"].apply(lambda x: extract_entities_using_regex(x))
    else:
        df["prompt"] = df["clean_text"].apply(lambda x: f"{TEMPLATE}\n{x}\n\nOutput:")
        result = llm_invoker.invoke_llms_df(df, "prompt")
        df["entities"] = result.progress_apply(lambda x: _extract_entities_from_json(str(x)))

    exploded_df = df.explode("entities")
    exploded_df[["entity_type", "entity_value"]] = pd.DataFrame(
        exploded_df["entities"].tolist(), index=exploded_df.index
    )
    df = exploded_df.drop(columns=["entities"])
    return df[["message_id", "entity_type", "entity_value"]]