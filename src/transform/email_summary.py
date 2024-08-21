import asyncio
import json
from typing import Dict, List, Union

import pandas as pd

from src.transform.llm_invoker import LLMInvoker

TEMPLATE = """
# Arabic and English Email Summarization

Your task is to summarize emails written in either Arabic or English. Regardless of the original language, always provide the summary in English. The summary should cover the important parts of the message, including:

1. The main purpose or topic of the email
2. Key points or requests
3. Any actionable items or deadlines
4. The overall tone (e.g., formal, urgent, friendly)

Keep the summary concise, ideally 2-3 sentences.

## Examples:

Input (English):
Subject: Quarterly Performance Review

Dear John,

I hope this email finds you well. As we approach the end of the quarter, it's time for our performance review meeting. I've scheduled our discussion for next Friday at 2 PM in the conference room.

Please come prepared with a summary of your key achievements this quarter, any challenges you've faced, and your goals for the next quarter. Also, don't forget to complete the self-assessment form by Wednesday.

If you need to reschedule, please let me know at least 24 hours in advance.

Best regards,
Sarah Johnson
HR Manager

Summary:
The email schedules a quarterly performance review meeting for next Friday at 2 PM. The employee (John) is asked to prepare a summary of achievements, challenges, and future goals, and to complete a self-assessment form by Wednesday. The tone is formal and instructive, with a clear deadline for rescheduling requests.

Input (Arabic):
Subject: دعوة لحضور ورشة عمل تدريبية

السادة الزملاء الكرام،

تحية طيبة وبعد،

يسرنا دعوتكم لحضور ورشة عمل تدريبية بعنوان "مهارات التواصل الفعال في بيئة العمل" والتي ستقام يوم الثلاثاء القادم الموافق 15 سبتمبر 2024 من الساعة 9 صباحاً وحتى 3 مساءً في قاعة التدريب الرئيسية.

الرجاء تأكيد حضوركم عبر الرد على هذا البريد الإلكتروني قبل يوم الخميس القادم. علماً بأن حضور هذه الورشة إلزامي لجميع رؤساء الأقسام.

نتطلع لمشاركتكم الفعالة.

مع أطيب التحيات،
قسم التدريب والتطوير

Summary:
The email invites employees to attend a mandatory training workshop on "Effective Communication Skills in the Workplace" on Tuesday, September 15, 2024, from 9 AM to 3 PM. Attendees are required to confirm their participation by replying to the email before next Thursday. The tone is formal and informative, emphasizing the importance of the workshop, especially for department heads.

Now, please summarize the following email in English:
"""


def summarize_messages(df: pd.DataFrame, llm_invoker: LLMInvoker) -> pd.DataFrame:
    df.loc[:, "prompt"] = df["clean_text"].apply(lambda x: f"{TEMPLATE}\n{x}\n\nSummary:")
    df.loc[:, "summary"] = llm_invoker.invoke_llms_df(df, "prompt")
    return df[["message_id", "summary"]]
