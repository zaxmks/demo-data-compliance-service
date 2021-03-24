from typing import List

import os
import numpy as np
import spacy

from src.dvc_artifact_handler import DVCArtifactHandler
from src.ner.named_entity import NamedEntity


class SpacyModel:
    def __init__(self, model="en_core_web_sm"):
        """Instantiate new NER model object."""
        self.model = spacy.load(
            DVCArtifactHandler.get_local_artifact(
                os.path.join("data/model_artifacts/spacy", model)
            )
        )

    def predict(self, text: str, confidence_threshold: float) -> List[NamedEntity]:
        """Make NER predictions on text."""
        pred = self.model(text)
        return [
            NamedEntity(str(e), e.start_char, e.end_char, e.label_, 0.9)
            for e in pred.ents
        ]
