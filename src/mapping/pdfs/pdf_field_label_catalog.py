import enum


class FieldLabelCatalog(enum.Enum):
    FULL_NAME = 1  # Full name in a single string
    SSN = 2  # Social security number
    DOB = 3  # Date of birth
    FULL_ADDRESS = 4  # Full address in a single string
    # those below here used for pseudofields
    FIRST_NAME = 5
    LAST_NAME = 6
    MIDDLE_NAME = 7
    STREET_ADDRESS = 8  # Street
    CITY = 9
    STATE = 10
    ZIP = 11
    COUNTRY = 12
