import os
import pytest

from src.config.database_config import DatabaseConfig


def test_load_env_matches_python_env(monkeypatch):
    monkeypatch.setenv("PYTHON_ENV", "local")
    config = DatabaseConfig.create_config_by_env()

    username, password, db, host, port = config.to_dict().values()

    assert username == "postgres"
    assert password == "password"
    assert db == "local_db"
    assert host == "postgres-db"
    assert port == "5432"


def test_load_env_fails_if_set_to_prod(monkeypatch):
    monkeypatch.setenv("PYTHON_ENV", "prod")
    with pytest.raises(Exception):
        DatabaseConfig.create_config_by_env()
