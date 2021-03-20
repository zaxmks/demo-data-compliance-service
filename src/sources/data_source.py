from src.sources.data_loader import DataLoader
from src.sources.structured_data_source import StructuredDataSource
from src.sources.unstructured_data_source import UnstructuredDataSource


class DataSource:
    def __new__(cls, data_source, client=None):
        """Build a new data source object while inferring input type."""
        return cls._build(data_source, client)

    @classmethod
    def _build(cls, data_source, client):
        """Build a new data source object."""
        data, structured, name = cls._load(data_source, client)
        if structured:
            return StructuredDataSource(data, name)
        else:
            return UnstructuredDataSource(data, name)

    @classmethod
    def _load(cls, data_source, client):
        """Load the data source."""
        return DataLoader(data_source, client).load()
