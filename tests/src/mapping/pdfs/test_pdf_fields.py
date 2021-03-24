import pandas as pd

from src.sources.structured_data_source import StructuredDataSource
from src.mapping.pdfs.pdf_field_name_classifier import FieldNameClassifier
from src.mapping.pdfs.pdf_field_label_catalog import FieldLabelCatalog
from src.mapping.pdfs.pseudofield_generator import PseudofieldGenerator


def test_basic_fields():
    data = {
        "tin": ["111-11-1111"],
        "dob": ["April 7, 1941"],
        "first_name": ["Bob"],
        "middle_initial": ["B."],
        "last_name": ["Bobson"],
        "address": ["16 Bobby Lane"],
        "city": ["Bobville"],
        "state": ["Bobalvania"],
        "zip": ["77007"],
        "country": ["Bobastan"],
    }
    df = pd.DataFrame(data)
    source = StructuredDataSource(data=df, name="test_pdf")
    pseudofield_generator = PseudofieldGenerator(source)
    pseudofield_generator.generate()
    id_info = FieldNameClassifier.get_id_info_from_df(source.get_data())
    assert id_info[FieldLabelCatalog.SSN].field_data[0] == "111-11-1111"
    assert id_info[FieldLabelCatalog.DOB].field_data[0] == "April 7, 1941"
    assert id_info[FieldLabelCatalog.FULL_NAME].field_data[0] == "Bob B. Bobson"
    address = id_info[FieldLabelCatalog.FULL_ADDRESS].field_data[0]
    val = "16 Bobby Lane Bobville Bobalvania 77007 Bobastan"
    assert address == val


def test_address():
    data = {
        "namesreported": ["Bob B. Bobson"],
        "ssn": ["111-11-1111"],
        "address": ["16 Bobby Lane, Bobville, Bobalvania, 77007, Bobstan"],
    }
    df = pd.DataFrame(data)
    source = StructuredDataSource(data=df, name="test_pdf")
    pseudofield_generator = PseudofieldGenerator(source)
    pseudofield_generator.generate()
    id_info = FieldNameClassifier.get_id_info_from_df(source.get_data())
    assert id_info[FieldLabelCatalog.SSN].field_data[0] == "111-11-1111"
    assert id_info[FieldLabelCatalog.FULL_NAME].field_data[0] == "Bob B. Bobson"
    address = id_info[FieldLabelCatalog.FULL_ADDRESS].field_data[0]
    val = "16 Bobby Lane, Bobville, Bobalvania, 77007, Bobstan"
    assert address == val
