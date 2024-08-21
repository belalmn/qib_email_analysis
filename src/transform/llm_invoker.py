import asyncio
import logging
from typing import Dict, List, Union

import pandas as pd
import torch
import transformers
from langchain_community.llms.ollama import Ollama
from langchain_huggingface import HuggingFacePipeline
from tqdm.asyncio import tqdm as atqdm
from tqdm import tqdm
from transformers import AutoTokenizer

tqdm.pandas()

class LLMInvoker:
    def __init__(self, model_name: str = "microsoft/Phi-3-mini-4k-instruct", use_ollama: bool = False):
        self.use_ollama = use_ollama
        self.model_name = model_name
        self.llm: Union[Ollama, HuggingFacePipeline]

        if use_ollama:
            logging.info(f"Using Ollama model: {model_name}")
            self.llm = Ollama(model=model_name)
        else:
            logging.info(f"Using HuggingFace model: {model_name}")
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            pipe = transformers.pipeline(
                task="text-generation",
                model=model_name,
                tokenizer=tokenizer,
                device="cuda" if torch.cuda.is_available() else "cpu",
                max_length=3000,
                torch_dtype=torch.bfloat16,
                trust_remote_code=True,
                do_sample=True,
            )
            self.llm = HuggingFacePipeline(pipeline=pipe)

    def invoke_llm(self, prompt: str) -> str:
        return self.llm.invoke(prompt)

    def invoke_llms_df(self, df: pd.DataFrame, prompt_column_name: str) -> pd.DataFrame:
        df["llm_response"] = df[prompt_column_name].progress_apply(self.invoke_llm)
        return df

    async def ainvoke_llm(self, prompt: str) -> str:
        return await self.llm.ainvoke(prompt)

    async def ainvoke_llms_df(self, df: pd.DataFrame, prompt_column_name: str) -> pd.DataFrame:
        tasks = [self.ainvoke_llm(row[prompt_column_name]) for _, row in df.iterrows()]
        results = await atqdm.gather(*tasks)
        df["llm_response"] = results
        return df["llm_response"]
