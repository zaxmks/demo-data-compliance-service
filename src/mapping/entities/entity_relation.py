from src.ner.named_entity import NamedEntity


class EntityRelation:
    def __init__(
        self,
        target_data_source,
        entity: NamedEntity,
        target_column_name: str,
        target_index: int,
        target_text: str,
        relation_confidence: float,
    ):
        """Initialize new entity relation."""
        self.target_data_source = target_data_source
        self.entity = entity
        self.target_column_name = target_column_name
        self.target_index = target_index
        self.target_text = target_text
        self.relation_confidence = relation_confidence

    def __str__(self):
        """Create string representation of entity relation."""
        return "'%s' -> '%s' (%s) conf: %.3f index: %d col: %s" % (
            str(self.entity),
            self.target_text,
            str(self.target_data_source),
            self.get_confidence(),
            self.target_index,
            self.target_column_name,
        )

    def get_confidence(self):
        """Return confidence of the relation."""
        # return self.entity.get_confidence() * self.relation_confidence
        return self.relation_confidence
