from __future__ import annotations

import os
from typing import List

from src.sources.data_source import DataSource


class DataCollection:
    def __init__(self, *args, **kwargs):
        """Instantate new data collection object."""
        self.sources = []
        self.unreadable_sources = []
        if len(args) > 0:
            self.append(*args, **kwargs)

    def __getitem__(self, i) -> List[DataSource]:
        """Get an item by index."""
        return self.sources[i]

    def __len__(self) -> int:
        """Get the length of the collection."""
        return len(self.sources)

    def _construct_filelist(
        self, data_directory, recursive=False, ignore_substring=None
    ) -> List[str]:
        """Get filenames from a directory with an option to recurse."""
        dir_name = os.path.abspath(data_directory)
        filelist = []
        for root, _, filenames in os.walk(dir_name):
            relative_path = ""
            if os.path.abspath(root) != os.path.abspath(dir_name):
                if recursive is False:
                    continue
                relative_path = os.path.abspath(root)[len(dir_name) + 1 :]
            for filename in filenames:
                full_filename = os.path.join(
                    os.path.join(dir_name, relative_path), filename
                )
                if ignore_substring is None or ignore_substring not in full_filename:
                    filelist.append(full_filename)

        return filelist

    def append(self, *args, **kwargs):
        """Append a new data source to this collection by specifying DataSource args."""
        if len(args) == 0:
            raise Exception("Must specify a valid input to append.")
        elif os.path.isdir(args[0]):
            file_list = self._construct_filelist(*args, **kwargs)
            for file_name in file_list:
                try:
                    self.sources.append(DataSource(file_name))
                except NotImplementedError:
                    self.unreadable_sources.append(file_name)
        else:
            self.sources.append(DataSource(*args, **kwargs))

    def extend(self, data_collection: DataCollection):
        self.sources.extend(data_collection.sources)

    def get_unstructured_sources(self) -> List[DataSource]:
        """Get all unstructured sources."""
        sources_to_return = []
        for source in self.sources:
            if not source.is_structured():
                sources_to_return.append(source)
        return sources_to_return

    def get_structured_sources(self) -> List[DataSource]:
        """Get all structured sources."""
        sources_to_return = []
        for source in self.sources:
            if source.is_structured():
                sources_to_return.append(source)
        return sources_to_return
