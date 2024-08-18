# from langchain_community.llms.ollama import Ollama

# llms = {}


# def invoke_llm(text: str, model: str = "llama2:7B") -> str:
#     if model not in llms:
#         try:
#             llms[model] = Ollama(model=model)
#         except Exception as e:
#             print(e)

#     llm = llms[model]
#     return llm.invoke(text)

import torch
import transformers
from transformers import AutoTokenizer

model = "meta-llama/Llama-3.1-8B-Instruct"
tokenizer = AutoTokenizer.from_pretrained(model)

pipeline = transformers.pipeline(
    task="text-generation",
    model=model,
    tokenizer=tokenizer,
    device="cuda" if torch.cuda.is_available() else "cpu",
    max_length=3000,
    torch_dtype=torch.bfloat16,
    trust_remote_code=True,
    do_sample=True,
)

def invoke_llm(text: str) -> str:
    return pipeline(text)[0]["generated_text"]
