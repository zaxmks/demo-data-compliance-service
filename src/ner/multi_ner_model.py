from typing import List

from src.ner.ner_model_factory import NERModelFactory

class MultiNERModel(object):

    def __init__(self, ner_configurations: List):
        self.ner_configurations = ner_configurations
        self.models = [NERModelFactory.get_model_from_config(config) for config in self.ner_configurations]

    def predict(self, text: str, confidence_threshold: float) -> List:
        """
        Gather the predictions from each independent model and distill final predictions
        """
        preds = [self.models[i].predict(text, self.ner_configurations[i].get_confidence_threshold()) for i in range(len(self.ner_configurations))]
        return self.resolve(preds, confidence_threshold, text)

    def resolve(self, preds:List, confidence_threshold:float, text:str):
        raise NotImplementedError("MultiNERModel.resolve is a pure virtual method")