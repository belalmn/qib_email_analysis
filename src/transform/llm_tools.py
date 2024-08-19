import torch
import transformers
from transformers import AutoTokenizer

model = "meta-llama/Llama-2-7b-chat-hf"
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
