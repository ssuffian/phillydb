import pytest


def pytest_addoption(parser):
    parser.addoption("--make-request", action="store_true", default=False)


@pytest.fixture
def test_opa_numbers():
    return ["883056605", "882391210"]


@pytest.fixture(params=["by_list", "by_sql"])
def opa_account_numbers(request, test_opa_numbers):
    if request.param == "by_list":
        return test_opa_numbers
    elif request.param == "by_sql":

        return f"""
            select parcel_number from opa_properties_public 
            where parcel_number in {tuple(test_opa_numbers)}
            """
