from src.mapping.values.value_match import ValueMatch


def test_init():
    vm = ValueMatch(0, 0.5, "test")
    assert vm.target_index == 0
    assert vm.confidence == 0.5
    assert vm.target_text == "test"


def test_to_dict():
    vm = ValueMatch(0, 0.5, "test")
    test_dict = vm.to_dict()
    assert test_dict["target_index"] == 0
    assert test_dict["confidence"] == 0.5
    assert test_dict["target_text"] == "test"
