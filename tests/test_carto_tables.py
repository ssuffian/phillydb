import pytest
import requests

from phillydb import carto_tables
from phillydb.testing_utils import MockCartoResponse


def maybe_monkeypatch_response(monkeypatch, pytestconfig, columns=None):
    columns = columns if columns else []
    if not pytestconfig.getoption("make_request"):

        def _fake_get(*args, **kwargs):
            return MockCartoResponse(columns)

        monkeypatch.setattr("requests.get", _fake_get)


@pytest.fixture(params=carto_tables.__all__)
def table_obj(request):
    TableClass = getattr(carto_tables, request.param)
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
    for link in table_obj.data_links:
        assert requests.get(link).status_code == 200


def test_query_by_single_str_column(monkeypatch, pytestconfig):
    property_obj = carto_tables.Properties()
    result_columns = ["location", "parcel_number"]
    maybe_monkeypatch_response(
        monkeypatch,
        pytestconfig,
        result_columns + property_obj.city_owned_prop_filter_cols,
    )
    df = property_obj.query_by_single_str_column(
        search_column="location",
        search_to_match="100",
        search_method="starts with",
        result_columns=result_columns,
        limit=10,
    )
    assert not df.empty

    license_obj = carto_tables.Licenses()
    result_columns = ["licensetype", "opa_account_num"]
    maybe_monkeypatch_response(
        monkeypatch,
        pytestconfig,
        result_columns + license_obj.city_owned_prop_filter_cols,
    )
    df = license_obj.query_by_single_str_column(
        search_column="licensetype",
        search_to_match="Re",
        search_method="starts with",
        result_columns=result_columns,
        limit=10,
    )
    assert not df.empty
