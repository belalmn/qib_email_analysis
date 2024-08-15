import numpy as np
import pandas as pd

from src.transform.llm_tools import invoke_llm

classes = [
    "customer_inquiry",
    "customer_complaint",
    "customer_feedback",
    "account_issue",
    "loan_application",
    "fraud_report",
    "technical_support",
    "product_information",
    "marketing_opt_out",
    "internal_communication",
    "partner_inquiry",
    "vendor_communication",
    "job_application",
    "press_inquiry",
    "regulatory_correspondence",
    "phishing_attempt",
    "spam",
    "service_request",
    "branch_information",
    "event_rsvp",
    "other",
]
class_set = set(classes)

template = """Based on the following email text, produce a classification of the email as one of the following:
    - 'customer_inquiry'
    - 'customer_complaint'
    - 'customer_feedback'
    - 'account_issue'
    - 'loan_application'
    - 'fraud_report'
    - 'technical_support'
    - 'product_information'
    - 'marketing_opt_out'
    - 'internal_communication'
    - 'partner_inquiry'
    - 'vendor_communication'
    - 'job_application'
    - 'press_inquiry'
    - 'regulatory_correspondence'
    - 'phishing_attempt'
    - 'spam'
    - 'service_request'
    - 'branch_information'
    - 'event_rsvp'
                    
    Do not provide any other text in the response other than the classification. Your answer should be one of the above classifications.
    If the message does not match any of the above classifications, respond with 'other'.
    Here are some examples, with the expected classification. Respond similarly.
                    
    Email text: 'I am writing to inquire about the status of my loan application.'
    Classification: loan_application
                    
    Email text: 'I am writing to complain about the poor service I received.'
    Classification: customer_complaint
                    
    Email text: 'I am writing to provide feedback on your product.'
    Classification: customer_feedback
"""


def get_classification(string: str) -> str:
    prompt = (
        template
        + f"""
    Email text: {string}
    Classification: """
    )
    result = invoke_llm(prompt)
    result = result.strip()
    if result not in class_set:
        # check if any of the classes are in the result
        result = next((c for c in classes if c in result), "missing")
    return result


def classify_messages(df: pd.DataFrame) -> pd.DataFrame:
    df["classification"] = df["plain_text_body"].apply(lambda x: get_classification(x))
    return df
