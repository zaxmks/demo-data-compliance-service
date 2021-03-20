import hashlib
import os
import tempfile

import pandas as pd
from pdfminer.high_level import extract_text

from src.clients.database_client import DatabaseClient
from src.clients.s3_client import S3Client
from src.sources.xml_parser import XMLParser


class DataLoader:
    def __init__(self, data_source, client):
        """Instantiate new data loader."""
        self.data_source = data_source
        self.client = client

    def load(self):
        """Load the data, inferring the type."""

        remote_name = None
        if isinstance(self.data_source, str) and isinstance(
            self.client, DatabaseClient
        ):
            data = self.client.read_sql("SELECT * FROM %s" % self.data_source)
            structured = True
            name = self.data_source
            return
        elif isinstance(self.data_source, str) and isinstance(self.client, S3Client):
            remote_name = str(self.data_source)
            self._sync_s3_source()
        if isinstance(self.data_source, str) and self.data_source.endswith(".csv"):
            data = pd.read_csv(self.data_source)
            structured = True
            name = remote_name if remote_name else self.data_source
        elif isinstance(self.data_source, str) and self.data_source.endswith(".tsv"):
            data = pd.read_csv(self.data_source, delimiter="\t")
            structured = True
            name = remote_name if remote_name else self.data_source
        elif isinstance(self.data_source, pd.DataFrame):
            data = self.data_source
            structured = True
            name = "pandas DataFrame (hash %d)" % (
                pd.util.hash_pandas_object(data).sum()
            )
        elif isinstance(self.data_source, str) and self.data_source.endswith(".txt"):
            data = self._read_raw_file(self.data_source)
            structured = False
            name = remote_name if remote_name else self.data_source
        elif isinstance(self.data_source, str) and self.data_source.endswith(".pdf"):
            data = self._read_pdf(self.data_source)
            structured = False
            name = remote_name if remote_name else self.data_source
        elif isinstance(self.data_source, str) and self.data_source.endswith(".xml"):
            data = XMLParser(self.data_source).get_dataframe()
            structured = True
            name = remote_name if remote_name else self.data_source
        elif isinstance(self.data_source, str) and (
            self.data_source.endswith(".xls") or self.data_source.endswith(".xlsx")
        ):
            sheets = pd.read_excel(self.data_source, sheet_name=None)
            structured = True
            if len(sheets) == 1:
                data = list(sheets.values())[0]
            else:
                raise NotImplementedError(
                    "Multi-sheet Excel files currently not supported."
                )
            name = remote_name if remote_name else self.data_source
        elif isinstance(self.data_source, str) and (self.client is None):
            data = self.data_source
            structured = False
            name = "string with hash %s" % self._string_hash(data)
        else:
            raise NotImplementedError(
                f"Attempt to initialize data source from unknown input type\nData Source: {self.data_source} type:{type(self.data_source)}"
            )

        return data, structured, name

    def _read_raw_file(self, filename) -> str:
        """Read a raw file as text."""
        with open(filename, "r") as fd:
            return fd.read()

    def _read_pdf(self, filename) -> str:
        """Read a PDF as raw text."""
        with open(filename, "rb") as fd:
            return extract_text(fd)

    def _sync_s3_source(self):
        """Sync s3 source, inferring whether directory or single file."""
        key_list = self.client.list_files(filter=self.data_source)
        if len(key_list) == 0:
            raise Exception("No S3 keys found matching %s", self.data_source)
        elif len(key_list) == 1:
            self.data_source = self.client.download_tmp_file(self.data_source)
        else:
            raise Exception(
                "Can only load one S3 key at a time, not directory %s"
                % self.data_source
            )

    def _string_hash(self, string: str):
        """Get md5sum for string input."""
        hasher = hashlib.md5()
        hasher.update(string.encode("utf-8"))
        return hasher.hexdigest()
