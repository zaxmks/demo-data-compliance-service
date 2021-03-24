import copy
import json

import pandas as pd
import xml.etree.ElementTree as ET


class XMLParser:
    def __init__(self, xml_filename):
        self.root = ET.parse(xml_filename).getroot()
        self.name = self.root.tag
        self.rows = []
        self.parsed = False

    def parse(self):
        """Parse the file."""
        self._get_rows()
        self._filter_rows()
        self._build_dataframe()
        self.parsed = True

    def get_dataframe(self):
        """Get a pandas dataframe containing the data within, parsing if need be."""
        if not self.parsed:
            self.parse()
        return self.df

    def _duplicate_and_append(self, a, to_append):
        """Add value to a copy of a list."""
        b = copy.deepcopy(a)
        b.append(to_append)
        return b

    def _merge_dict(self, a, b):
        """Merge two dictionaries."""
        c = copy.deepcopy(a)
        c.update(b)
        return c

    def _expand_child(self, child_object, curr_dict, curr_depth, curr_tags):
        """Expand a child while keeping track of all parent data."""
        curr_dict = self._merge_dict(curr_dict, child_object.attrib)
        if curr_depth > 1:
            curr_tags = self._duplicate_and_append(curr_tags, child_object.tag)
        for child in child_object:
            if len(child) > 0:
                self._expand_child(child, curr_dict, curr_depth + 1, curr_tags)
            else:
                curr_dict = self._merge_dict(curr_dict, {child.tag: child.text})
        curr_dict["__tags__"] = curr_tags
        self.rows.append(curr_dict)

    def _get_rows(self):
        """Get all row data."""
        self._expand_child(self.root, self.root.attrib, 1, [])

    def _filter_rows(self):
        """Filter invalid rows."""
        self.new_rows = []
        for row in self.rows:
            if len(row.keys()) > 1:
                self.new_rows.append(row)
        self.rows = self.new_rows

    def _build_dataframe(self):
        """Build a pandas dataframe from the collected data."""
        self.df = pd.DataFrame(self.rows)
