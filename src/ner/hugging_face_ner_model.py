from typing import List

import os
import numpy as np
from transformers import pipeline
from transformers import BertForTokenClassification
from transformers import PreTrainedTokenizerFast

from src.dvc_artifact_handler import DVCArtifactHandler
from src.ner.named_entity import NamedEntity

class HuggingFaceNERModel(object):
    def __init__(self):
        self.pipeline = pipeline("ner")

    def predict(self, text: str, confidence_threshold: float) -> List[NamedEntity]:
        pred = self.pipeline(text)
        return [
            NamedEntity(p["word"], p["start"], p["end"], label="PERSON", confidence=p["score"])
            for p in pred
        ]