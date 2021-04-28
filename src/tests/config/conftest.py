from src.config.settings import load_env


def pytest_runtest_setup():
    load_env()
