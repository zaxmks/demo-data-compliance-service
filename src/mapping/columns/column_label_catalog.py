import enum


class ColumnLabelCatalog(enum.Enum):
    FULL_NAME = 1  # Full name in a single string
    SSN = 2  # Social security number
    DOB = 3  # Date of birth
    EMAIL = 4  # Email address
    FIRST_NAME = 5  # First name
    MIDDLE_NAME = 6  # Middle name
    LAST_NAME = 7  # Last name
