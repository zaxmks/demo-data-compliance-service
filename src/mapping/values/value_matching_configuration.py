import hashlib
import json


class ValueMatchingConfiguration:
    def __init__(
        self,
        map_by: str = "name",
        confidence_threshold: float = 0.5,
        model_type: str = "embedding",
        ignore_case: bool = True,
        ignore_special_characters: bool = True,
        ignore_digits: bool = True,
        **model_config
    ):
        """Instantiate new mapping configuration object."""
        self.map_by = map_by
        self.confidence_threshold = confidence_threshold
        self.model_type = model_type
        self.ignore_case = ignore_case
        self.ignore_special_characters = ignore_special_characters
        self.ignore_digits = ignore_digits
        self.model_config = model_config

    def get_map_by_type(self) -> str:
        """Get the mapping type ("value" or "name")."""
        return self.map_by

    def get_confidence_threshold(self) -> float:
        """Get the confidence threshold."""
        return self.confidence_threshold

    def get_model_type(self) -> str:
        """Get the model type."""
        return self.model_type

    def get_model_config(self) -> dict:
        """Return dictionary of model parameters."""
        return self.model_config

    def to_dict(self) -> dict:
        """Convert parameters to a dictionary."""
        return {
            "map_by": self.map_by,
            "confidence_threshold": self.confidence_threshold,
            "model_type": self.model_type,
            "ignore_case": self.ignore_case,
            "ignore_special_characters": self.ignore_special_characters,
            "ignore_digits": self.ignore_digits,
            "model_config": self.model_config,
        }

    def to_json(self, filename):
        """Write configuration to json file."""
        with open(filename, "w") as fd:
            json.dump(self.to_dict(), fd)

    def from_dict(self, raw_dict):
        """Load parameters from a dictionary."""
        self.map_by = raw_dict["map_by"]
        self.confidence_threshold = raw_dict["confidence_threshold"]
        self.model_type = raw_dict["model_type"]
        self.ignore_case = raw_dict["ignore_case"]
        self.ignore_special_characters = raw_dict["ignore_special_characters"]
        self.ignore_digits = raw_dict["ignore_digits"]
        self.model_config = raw_dict["model_config"]

    def from_json(self, filename):
        """Read a configuration from a json file."""
        with open(filename, "r") as fd:
            raw_dict = json.load(fd)
        self.from_dict(raw_dict)

    def get_fingerprint(self):
        """Return unique fingerprint signature for this configuration."""
        return hashlib.md5(
            str(json.dumps(self.to_dict(), sort_keys=True)).encode()
        ).hexdigest()
