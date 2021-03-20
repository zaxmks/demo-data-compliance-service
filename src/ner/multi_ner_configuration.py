import hashlib
import json

from typing import List

from src.ner.ner_configuration import NERConfiguration

class MultiNERConfiguration(object):

    def __init__(self,
        confidence_threshold: float = 0.5,
        resolution_algorithm: str = "simple_voting",
        model_configs: List[NERConfiguration] = []
    ):
        self.resolution_algorithm = resolution_algorithm
        self.confidence_threshold = confidence_threshold
        self.model_configs = [mc if type(mc) == NERConfiguration else NERConfiguration(**mc) for mc in model_configs]

    def get_confidence_threshold(self) -> float:
        return self.confidence_threshold

    def get_resolution_algorithm(self) -> str:
        return self.resolution_algorithm

    def to_dict(self) -> dict:
        return {
            "confidence_threshold": self.confidence_threshold,
            "resolution_algorithm": self.resolution_algorithm,
            "model_configs": [config.to_dict() for config in self.model_configs]
        }

    def to_json(self, filename: str):
        with open(filename, "w") as fd:
            json.dump(self.to_dict(), fd)

    def from_dict(self, raw_dict: dict):
        self.confidence_threshold = raw_dict["confidence_threshold"]
        self.resolution_algorithm = raw_dict["resolution_algorithm"]
        self.model_configs = [NERConfiguration() for config in raw_dict["model_configs"]]
        for i in range(len(raw_dict["model_configs"])):
            self.model_configs[i].from_dict(raw_dict["model_configs"][i])

    def from_json(self, filename: str):
        with open(filename, "r") as fd:
            raw_dict = json.load(fd)
        self.from_dict(raw_dict)

    def get_fingerprint(self):
        return hashlib.md5(
            str(json.dumps(self.to_dict(), sort_keys=True)).encode()
        ).hexdigest()

