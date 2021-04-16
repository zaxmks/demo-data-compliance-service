from typing import Any


class DataSourceBase:
    def __init__(self, data: Any, structured: bool, name: str):
        """Initialize a generic data source."""
        self.data = data
        self.structured = structured
        self.name = name

    def __str__(self) -> str:
        """Get name of this object."""
        return self.name

    def __repr__(self) -> str:
        """Get string representation."""
        return self.name

    def __len__(self) -> int:
        """Get length of data source (structured or unstructured)."""
        return len(self.get_data())

    def is_structured(self) -> bool:
        """Detect if the data source is structured."""
        return self.structured

    def get_data(self):
        """Return data from the source. If structured, a pandas dataframe is returned. If unstructured,
        the full text is returned as a string.
        """
        return self.data
