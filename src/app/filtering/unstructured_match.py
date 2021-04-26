import logging

logger = logging.getLogger(__name__)


class UnstructuredMatch:
    FIRST_NAME = "first_name"
    LAST_NAME = "last_name"
    SSN = "ssn"
    DATE_OF_BIRTH = "date_of_birth"

    def __init__(self):
        self.match_dict = {
            self.FIRST_NAME: True,
            self.LAST_NAME: True,
            self.SSN: False,
            self.DATE_OF_BIRTH: False,
        }

    def set_true(self, key):
        if key in self.match_dict:
            self.match_dict[key] = True
        else:
            logger.warning(f"Could not set {key} to {True}, key not present")

    def set_false(self, key):
        if key in self.match_dict:
            self.match_dict[key] = False
        else:
            logger.warning(f"Could not set {key} to {False}, key not present")

    def is_match(self, key):
        if key in self.match_dict:
            return self.match_dict[key]
        else:
            logger.warning(f"Could not read {key}, key not present")

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.match_dict)

