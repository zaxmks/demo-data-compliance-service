from src.entity_label_catalog import EntityLabelCatalog


class NamedEntity:
    def __init__(
        self, text: str, start_char: int, end_char: int, label: str, confidence: float
    ):
        """Instantiate new text entity."""
        if label not in EntityLabelCatalog.get_names():
            raise Exception(
                "Cannot instantiate entity, '%s' not found in label catalog" % label
            )
        self.text = text
        self.start_char = start_char
        self.end_char = end_char
        self.label = label
        self.confidence = confidence

    def __str__(self) -> str:
        """Return string representation of entity."""
        return "'%s' conf: %.3f" % (self.text, self.confidence)

    def __repr__(self) -> str:
        """Return representation of entity."""
        return self.__str__()

    def get_start_char(self) -> int:
        """Return the start character number."""
        return self.start_char

    def get_end_char(self) -> int:
        """Return the end character number."""
        return self.end_char

    def get_text(self) -> str:
        """Return text object."""
        return self.text

    def get_label(self) -> str:
        """Return label."""
        return self.label

    def get_confidence(self) -> float:
        """Return the confidence."""
        return self.confidence

    def to_tuple(self) -> tuple:
        """Return a tuple of entity parameters."""
        return (self.text, self.start_char, self.end_char, self.label, self.confidence)

    def to_displacy_dict(self) -> dict:
        """Return a dict for input to displacy visualization."""
        return {
            "start": self.start_char,
            "end": self.end_char,
            "label": self.get_label(),
        }
