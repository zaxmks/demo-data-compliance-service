from src.config.settings import current_env, is_deployed_env, is_local_env


def test_python_env_is_read_by_current_env(monkeypatch):
    monkeypatch.setenv("PYTHON_ENV", "local")
    env = current_env()
    assert "local" == env, "python_env being set is being ignored"


def test_can_identify_local_env_1(monkeypatch):
    monkeypatch.setenv("PYTHON_ENV", "local")
    assert is_local_env() is True


def test_can_identify_local_env_2(monkeypatch):
    monkeypatch.setenv("PYTHON_ENV", "prod")
    assert is_local_env() is False


def test_can_identify_deployed_env_1(monkeypatch):
    monkeypatch.setenv("PYTHON_ENV", "prod")
    assert is_deployed_env() is True


def test_can_identify_deployed_env_2(monkeypatch):
    monkeypatch.setenv("PYTHON_ENV", "local")
    assert is_deployed_env() is False
