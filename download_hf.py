from transformers import AutoModelForSequenceClassification
from huggingface_hub import login
import os
from tqdm.auto import tqdm

models = [
    # "lxyuan/distilbert-base-multilingual-cased-sentiments-student",
    # "meta-llama/Llama-2-7b-chat-hf",
    "meta-llama/Meta-Llama-3.1-8B-Instruct",
    # "sshleifer/distilbart-cnn-12-6",
    # "Helsinki-NLP/opus-mt-ar-en",
    # "facebook/bart-large-mnli"
]

model_dir = "models"
for model in tqdm(models):
    print(f"Downloading {model}")
    modelName = model.split("/")[-1]
    modelPath = os.path.join(model_dir, modelName)
    model = AutoModelForSequenceClassification.from_pretrained(model)
    model.save_pretrained(modelPath)
    print(f"Saved to {modelPath}")