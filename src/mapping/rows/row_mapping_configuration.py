import hashlib
import json


class RowMappingConfiguration:
    def __init__(
        self,
        confidence_threshold: float = 0.5,
        model_type: str = "weighted_linear",
        **model_config
    ):
        self.confidence_threshold = confidence_threshold
        self.model_type = model_type
        self.model_config = model_config

    def get_confidence_threshold(self):
        return self.confidence_threshold

    def get_model_type(self):
        return self.model_type

    def get_model_config(self):
        return self.model_config

    def to_dict(self) -> dict:
        """Return a dictionary of row mapping configuration parameters."""
        return {
            "confidence_threshold": self.confidence_threshold,
            "model_type": self.model_type,
            "model_config": self.model_config,
        }

    def to_json(self, filename: str):
        """Write class to JSON file."""
        with open(filename, "w") as fd:
            json.dump(self.to_dict(), fd)

    def from_dict(self, raw_dict):
        """Load parameters from a dictionary."""
        self.confidence_threshold = raw_dict["confidence_threshold"]
        self.model_type = raw_dict["model_type"]
        self.model_config = raw_dict["model_config"]

    def from_json(self, filename):
        """Read a configuration from a JSON file."""
        with open(filename, "r") as fd:
            raw_dict = json.load(fd)
        self.from_dict(raw_dict)

    def get_fingerprint(self):
        """Get unique fingerprint signature for this configuration."""
        return hashlib.md5(
            str(json.dumps(self.to_dict(), sort_keys=True)).encode()
        ).hexdigest()
