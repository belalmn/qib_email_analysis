from langchain_community.llms.ollama import Ollama

llms = {}


def invoke_llm(text: str, model: str = "llama2:7B") -> str:
    if model not in llms:
        try:
            llms[model] = Ollama(model=model)
        except Exception as e:
            print(e)

    llm = llms[model]
    return llm.invoke(text)
