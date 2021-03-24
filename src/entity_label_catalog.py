from enum import IntEnum


class EntityLabelCatalog(IntEnum):
    """Enumerated named entity labels."""

    # spaCy default labels
    PERSON = 0  # People, including fictional
    NORP = 1  # Nationalities or religious or political groups
    FAC = 2  # Buildings, airports, highways, bridges, etc.
    ORG = 3  # Companies, agencies, institutions, etc.
    GPE = 4  # Countries, sities, states.
    LOC = 5  # Non-GPE locations, mountain ranges, bodies of water
    PRODUCT = 6  # Objects, vehicles, foods, etc. (Not services.)
    EVENT = 7  # Named hurricanes, battles, wars, sports events, etc.
    WORK_OF_ART = 8  # Titles of books, songs, etc.
    LAW = 9  # Named documents made into laws.
    LANGUAGE = 10  # Any named language.
    DATE = 11  # Absolute or relative dates or periods.
    TIME = 12  # Times smaller than a day.
    PERCENT = 13  # Percentage, including ”%“.
    MONEY = 14  # Monetary values, including unit.
    QUANTITY = 15  # Measurements, as of weight or distance.
    ORDINAL = 16  # “first”, “second”, etc.
    CARDINAL = 17  # Numerals that do not fall under another type.

    # Additional VK labels
    SSN = 18  # Social security number
    EMAIL_ADDRESS = 19  # Email address
    PHYSICAL_ADDRESS = 20  # Physical address (any format)
    PHONE_NUMBER = 21  # Phone number (any format)

    @classmethod
    def get_names(cls):
        """Return names of all classes."""
        return [label.name for label in cls]

    @classmethod
    def get_spacy_labels(cls):
        """Return names of all spaCy labels."""
        return [label.name for i, label in enumerate(cls) if i <= 17]

    @classmethod
    def get_vk_labels(cls):
        """Return names of all VK custom labels."""
        return [label.name for i, label in enumerate(cls) if i > 17]
