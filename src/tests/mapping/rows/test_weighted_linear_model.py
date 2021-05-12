from mock import Mock

from src.mapping.values.value_match import ValueMatch
from src.mapping.rows.weighted_linear_model import WeightedLinearModel

config = {"weights": {"email": 1, "name": 3, "ssn": 6}, "null_confidence": 0.5}


def test_init():
    wlm = WeightedLinearModel(**config)
    assert wlm.weights["email"] == 1
    assert wlm.null_confidence == 0.5


def test_predict_perfect():
    wlm = WeightedLinearModel(**config)
    column_relations = [Mock(), Mock(), Mock()]
    column_relations[0].target_column_name = "email"
    column_relations[1].target_column_name = "name"
    column_relations[2].target_column_name = "ssn"
    value_match_group = [
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="reed.coke@enron.com",
            source_column="email_address",
            target_column="email",
        ),
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="Reed A. Coke",
            source_column="fullname",
            target_column="name",
        ),
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="123-45-6789",
            source_column="social",
            target_column="ssn",
        ),
    ]
    confidence = wlm.predict(column_relations, value_match_group)
    assert confidence > 0.99


def test_predict_with_null_match():
    wlm = WeightedLinearModel(**config)
    column_relations = [Mock(), Mock(), Mock()]
    column_relations[0].target_column_name = "email"
    column_relations[1].target_column_name = "name"
    column_relations[2].target_column_name = "ssn"
    value_match_group = [
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="reed.coke@enron.com",
            source_column="email_address",
            target_column="email",
        ),
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="Reed A. Coke",
            source_column="fullname",
            target_column="name",
        ),
    ]
    confidence = wlm.predict(column_relations, value_match_group)
    assert confidence == 0.7


def test_predict_with_unweighted_column():
    wlm = WeightedLinearModel(**config)
    column_relations = [Mock(), Mock(), Mock(), Mock()]
    column_relations[0].target_column_name = "email"
    column_relations[1].target_column_name = "name"
    column_relations[2].target_column_name = "ssn"
    column_relations[3].target_column_name = "unweighted_column"
    value_match_group = [
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="reed.coke@enron.com",
            source_column="email_address",
            target_column="email",
        ),
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="Reed A. Coke",
            source_column="fullname",
            target_column="name",
        ),
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="123-45-6789",
            source_column="social",
            target_column="ssn",
        ),
        ValueMatch(
            target_index=1,
            confidence=1,
            target_text="text from unweighted column",
            source_column="unweughted_column",
            target_column="unweighted_column",
        ),
    ]
    confidence = wlm.predict(column_relations, value_match_group)
    assert confidence > 0.99