from typing import Dict, List, Union

import pandas as pd
import json

from src.transform.llm_tools import invoke_llm

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


def extract_entities_from_messages(df: pd.DataFrame) -> pd.DataFrame:
    def _extract_entities_from_message(message: str) -> Dict[str, Union[str, List[str]]]:
        prompt = f"{TEMPLATE}\n{message}\n\nOutput:"
        result = invoke_llm(prompt)
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return {}

    df["entities"] = df["clean_text"].progress_apply(lambda x: _extract_entities_from_message(str(x)))
    return df
