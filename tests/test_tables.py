import pytest
import requests

from phillydb import tables


def maybe_monkeypatch_response(monkeypatch, pytestconfig, columns=None):
    columns = columns if columns else []
    if not pytestconfig.getoption("make_request"):

        def _fake_get(*args, **kwargs):
            return MockResponse(columns)

        monkeypatch.setattr("requests.get", _fake_get)


class MockResponse:
    def __init__(self, cols):
        self.cols = cols
        self.status_code = 200
        pass

    def json(self):
        columns_dict = {c: "2020-01-01 12:00:00" for c in self.cols}
        return {"rows": [columns_dict], "fields": columns_dict}


@pytest.fixture(params=["by_list", "by_sql"])
def opa_account_numbers(request):
    test_opa_numbers = ["883056605", "882391210", "884466340", "888081661"]
    if request.param == "by_list":
        return test_opa_numbers
    elif request.param == "by_sql":

        return f"""
            select parcel_number from opa_properties_public 
            where parcel_number in {tuple(test_opa_numbers)}
            """


@pytest.fixture(params=tables.__all__)
def table_obj(request):
    TableClass = getattr(tables, request.param)
    # Iterates through each table class as a parameterized pytest fixture
    return TableClass()


def test_all_tables_query_by_opa_account_numbers(
    opa_account_numbers, table_obj, pytestconfig, monkeypatch
):
    maybe_monkeypatch_response(
        monkeypatch,
        pytestconfig,
        table_obj.default_columns
        + table_obj.default_opa_properties_public_joined_columns,
    )
    df = table_obj.query_by_opa_account_numbers(opa_account_numbers=opa_account_numbers)
    if df.empty:
        raise AssertionError(f"{table_class.title} failed to return a dataframe.")


def test_all_tables_metadata_urls(
    opa_account_numbers, table_obj, pytestconfig, monkeypatch
):
    maybe_monkeypatch_response(monkeypatch, pytestconfig)
    assert requests.get(table_obj.get_cartodb_link()).status_code == 200
    if table_obj.get_odb_link():
        assert requests.get(table_obj.get_odb_link()).status_code == 200
