# import transformers
# from transformers import AutoModelForSequenceClassification, AutoTokenizer

# model = AutoModelForSequenceClassification.from_pretrained("microsoft/Phi-3-mini-4k-instruct")

# model.save_pretrained("../models/")

from transformers import pipeline

pipe = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

pipe.save_pretrained("../models/bart-large-mnli")
