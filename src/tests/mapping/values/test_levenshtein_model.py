import numpy as np
import pandas as pd

from src.mapping.values.levenshtein_model import LevenshteinModel
from src.mapping.values.value_matching_target import ValueMatchingTarget

test_series = pd.read_csv("src/tests/test_data/sample/names.csv")["name"]
test_target = ValueMatchingTarget(test_series)


def test_predict_when_above_threshold():
    lm = LevenshteinModel()
    matches = lm.predict("Sue Hong", test_target, 0.5)
    assert len(matches) == 1
    assert matches[0].target_index == 0
    assert matches[0].target_text == "Soo Hong"
    assert np.isclose(round(matches[0].confidence, 6), 0.510204)
