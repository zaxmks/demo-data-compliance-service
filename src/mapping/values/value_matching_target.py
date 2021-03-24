from typing import List, Optional

import pandas as pd

from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration


class ValueMatchingTarget:
    def __init__(
        self,
        data_series: Optional[pd.Series] = None,
        config: Optional[ValueMatchingConfiguration] = None,
    ):
        """Instantiate new matching target object."""
        if config is None:
            config = ValueMatchingConfiguration()
        self.data_series = data_series
        self.ignore_case = config.ignore_case
        self.ignore_special_characters = config.ignore_special_characters
        self.ignore_digits = config.ignore_digits
        self.preprocessed_targets = []
        if self.data_series is not None:  # if just want to call preprocess
            self._prepare_target()

    def _prepare_target(self):
        """Use the current configuration to preprocess the target."""
        self.preprocessed_targets = []
        for value in self.data_series:
            self.preprocessed_targets.append(self.preprocess_string(value))

    def preprocess_string(self, text: str) -> str:
        """Preprocess a string given the model configuration."""
        newtext = str(text)
        if self.ignore_case:
            newtext = newtext.lower()
        if self.ignore_special_characters:
            newtext = "".join(c for c in newtext if c.isalnum())
        if self.ignore_digits:
            newtext = "".join([c for c in newtext if not c.isdigit()])

        return newtext

    def get_preprocessed_targets(self) -> List[str]:
        """Return the pandas series for the underlying data."""
        return self.preprocessed_targets

    def get_unprocessed_targets(self) -> List[str]:
        """Return the pandas series for the underlying data."""
        return self.data_series

    def get_target_index(self, row_index: int) -> int:
        if isinstance(self.data_series, pd.Series):
            return self.data_series.index[row_index]
        else:
            return row_index
