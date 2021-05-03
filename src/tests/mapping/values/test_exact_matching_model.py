import pandas as pd

from src.mapping.values.exact_matching_model import ExactMatchingModel
from src.mapping.values.value_matching_target import ValueMatchingTarget

test_series = pd.read_csv("src/tests/test_data/sample/names.csv")["name"]
test_target = ValueMatchingTarget(test_series)


def test_predict_when_above_threshold():
    emm = ExactMatchingModel()
    matches = emm.predict("soo hong", test_target, 0.5)
    assert len(matches) == 1
    assert matches[0].target_index == 0
    assert matches[0].target_text == "Soo Hong"
    assert matches[0].confidence == 1.0


def test_predict_when_below_threshold():
    emm = ExactMatchingModel()
    matches = emm.predict("soo hong", test_target, 1.1)
    assert len(matches) == 0


def test_predict_when_no_match():
    emm = ExactMatchingModel()
    matches = emm.predict("this will not have a match", test_target, 0.5)
    assert len(matches) == 0
