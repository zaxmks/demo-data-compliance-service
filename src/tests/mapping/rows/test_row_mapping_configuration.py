import json
import os
import tempfile

from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration


def get_custom_mapping_configuration():
    return RowMappingConfiguration(
        confidence_threshold=0.1234, model_type="mttest", model="config", value=0.9
    )


def get_custom_dict_configuration():
    return {
        "confidence_threshold": 0.1234,
        "model_type": "mttest",
        "model_config": {"model": "config", "value": 0.9},
    }


def test_init():
    mc = RowMappingConfiguration()
    assert mc.confidence_threshold == 0.5
    assert mc.model_type == "weighted_linear"
    assert mc.model_config == {}


def test_get_confidence_threshold():
    mc = RowMappingConfiguration(confidence_threshold=0.4321)
    assert mc.get_confidence_threshold() == 0.4321


def test_get_model_type():
    mc = RowMappingConfiguration(model_type="test")
    assert mc.get_model_type() == "test"


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
    mc = RowMappingConfiguration()
    mc.from_dict(test_data)
    assert mc.confidence_threshold == 0.1234
    assert mc.model_type == "mttest"
    assert mc.get_model_config() == {"model": "config", "value": 0.9}


def test_from_json():
    tempdir = tempfile.TemporaryDirectory()
    tmpfilename = os.path.join(tempdir.name, "test.json")
    with open(tmpfilename, "w") as fd:
        json.dump(get_custom_dict_configuration(), fd)
    mc = RowMappingConfiguration()
    mc.from_json(tmpfilename)
    assert mc.confidence_threshold == 0.1234
    assert mc.model_type == "mttest"
    assert mc.get_model_config() == {"model": "config", "value": 0.9}
