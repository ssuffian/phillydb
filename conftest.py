def pytest_addoption(parser):
    parser.addoption("--make-request", action="store_true", default=False)


