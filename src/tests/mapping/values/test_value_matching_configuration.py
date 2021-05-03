import json
import os
import tempfile

from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration


def get_custom_mapping_configuration():
    return ValueMatchingConfiguration(
        map_by="mbtest",
        confidence_threshold=0.1234,
        model_type="mttest",
        ignore_case=False,
        ignore_special_characters=False,
        ignore_digits=False,
        modelparam1=0.4321,
        modelparam2="mp2test",
    )


def get_custom_dict_configuration():
    return {
        "map_by": "mbtest",
        "confidence_threshold": 0.1234,
        "model_type": "mttest",
        "ignore_case": False,
        "ignore_special_characters": False,
        "ignore_digits": False,
        "model_config": {"modelparam1": 0.4321, "modelparam2": "mp2test"},
    }


def test_init():
    mc = ValueMatchingConfiguration()
    assert mc.map_by == "name"
    assert mc.confidence_threshold == 0.5
    assert mc.model_type == "embedding"
    assert mc.ignore_case
    assert mc.ignore_special_characters
    assert mc.ignore_digits
    assert mc.model_config == {}


def test_get_map_by_type():
    mc = ValueMatchingConfiguration(map_by="test")
    assert mc.get_map_by_type() == "test"


def test_get_confidence_threshold():
    mc = ValueMatchingConfiguration(confidence_threshold=0.1234)
    assert mc.get_confidence_threshold() == 0.1234


def test_get_model_type():
    mc = ValueMatchingConfiguration(model_type="test")
    assert mc.get_model_type() == "test"


def test_get_model_config():
    mc = ValueMatchingConfiguration(testarg1="test1", testarg2="test2")
    assert mc.get_model_config() == {"testarg1": "test1", "testarg2": "test2"}


def test_to_dict():
    mc = get_custom_mapping_configuration()
    assert mc.to_dict() == get_custom_dict_configuration()


def test_to_json():
    mc = get_custom_mapping_configuration()
    tempdir = tempfile.TemporaryDirectory()
    tmpfilename = os.path.join(tempdir.name, "test.json")
    mc.to_json(tmpfilename)
    with open(tmpfilename, "r") as fd:
        test_data = json.load(fd)
    assert test_data == get_custom_dict_configuration()


def test_from_dict():
    test_data = get_custom_dict_configuration()
    mc = ValueMatchingConfiguration()
    mc.from_dict(test_data)
    assert mc.map_by == "mbtest"
    assert mc.confidence_threshold == 0.1234
    assert mc.model_type == "mttest"
    assert mc.ignore_case == False
    assert mc.ignore_special_characters == False
    assert mc.ignore_digits == False
    assert mc.model_config == {"modelparam1": 0.4321, "modelparam2": "mp2test"}


def test_from_json():
    tempdir = tempfile.TemporaryDirectory()
    tmpfilename = os.path.join(tempdir.name, "test.json")
    with open(tmpfilename, "w") as fd:
        json.dump(get_custom_dict_configuration(), fd)
    mc = ValueMatchingConfiguration()
    mc.from_json(tmpfilename)
    assert mc.map_by == "mbtest"
    assert mc.confidence_threshold == 0.1234
    assert mc.model_type == "mttest"
    assert mc.ignore_case == False
    assert mc.ignore_special_characters == False
    assert mc.ignore_digits == False
    assert mc.model_config == {"modelparam1": 0.4321, "modelparam2": "mp2test"}
