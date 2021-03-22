import pytest
import requests

from phillydb import carto_tables
from phillydb.testing_utils import MockCartoResponse, maybe_monkeypatch_response


@pytest.fixture(params=carto_tables.__all__)
def table_obj(request):
    TableClass = getattr(carto_tables, request.param)
    # Iterates through each table class as a parameterized pytest fixture
    return TableClass()


def test_all_tables_list(table_obj, pytestconfig, monkeypatch):
    output_rows = [{"ABC": "DEF"}]
    maybe_monkeypatch_response(
        monkeypatch, pytestconfig, output_rows,
    )
    df = table_obj.list(limit=1)
    if df.empty:
        raise AssertionError(f"{table_obj.title} failed to return a dataframe.")


def test_all_tables_query_by_opa_account_numbers(
    opa_account_numbers, table_obj, pytestconfig, monkeypatch
):
    columns = (
        table_obj.default_columns
        + table_obj.default_opa_properties_public_joined_columns
    )
    output_rows = [{c: "2020-01-01 12:00:00" for c in columns}]
    maybe_monkeypatch_response(
        monkeypatch, pytestconfig, output_rows,
    )
    df = table_obj.query_by_opa_account_numbers(opa_account_numbers=opa_account_numbers)
    if df.empty:
        raise AssertionError(f"{table_obj.title} failed to return a dataframe.")


def test_all_tables_metadata_urls(
    opa_account_numbers, table_obj, pytestconfig, monkeypatch
):
    maybe_monkeypatch_response(monkeypatch, pytestconfig)
    for link in table_obj.data_links:
        assert requests.get(link).status_code == 200


def test_query_by_single_str_column(monkeypatch, pytestconfig):
    property_obj = carto_tables.Properties()
    result_columns = ["location", "parcel_number"]
    columns = result_columns + property_obj.city_owned_prop_filter_cols
    output_rows = [{c: "2020-01-01 12:00:00" for c in columns}]
    output_rows[0]["parcel_number"] = "1234"
    maybe_monkeypatch_response(
        monkeypatch, pytestconfig, output_rows,
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
    columns = result_columns + license_obj.city_owned_prop_filter_cols

    output_rows = [{c: "2020-01-01 12:00:00" for c in columns}]
    output_rows[0]["opa_account_num"] = "1234"

    maybe_monkeypatch_response(monkeypatch, pytestconfig, output_rows)
    df = license_obj.query_by_single_str_column(
        search_column="licensetype",
        search_to_match="Re",
        search_method="starts with",
        result_columns=result_columns,
        limit=10,
    )
    assert not df.empty


def test_guess_property_ownership(monkeypatch, pytestconfig):
    rtt_obj = carto_tables.RealEstateTransfers()
    opa_account_number = "883054500"
    maybe_monkeypatch_response(
        monkeypatch,
        pytestconfig,
        [
            {
                "opa_account_num": opa_account_number,
                "grantees": "COMMONWEALTH OF PENNSYLVANIA; DEPARTMENT OF TRANSPORTATION",
                "grantors": "PHILADELPHIA ELECTRIC COMPANY",
                "recording_date": "2016-06-12",
                "address_low": "2301",
                "address_low_suffix": None,
                "address_high": None,
                "address_low_frac": None,
                "street_predir": None,
                "street_name": "MARKET",
                "street_suffix": "ST",
                "street_address": "2301 MARKET ST",
            },
            {
                "opa_account_num": opa_account_number,
                "grantees": "",
                "grantors": "",
                "recording_date": "2000-01-01",
                "address_low": "BAD ADDRESS",
                "address_low_suffix": None,
                "address_high": None,
                "address_low_frac": None,
                "street_predir": None,
                "street_name": "MARKET",
                "street_suffix": "ST",
                "street_address": "2301 MARKET ST",
            },
        ],
    )
    recording_date = "2015-06-09"
    owner_dict = rtt_obj.infer_property_ownership(
        opa_account_number=opa_account_number, recording_date=recording_date
    )
    assert owner_dict["owner"] == "PHILADELPHIA ELECTRIC COMPANY"

    recording_date = "2020-06-09"
    owner_dict = rtt_obj.infer_property_ownership(
        opa_account_number=opa_account_number, recording_date=recording_date
    )
    assert (
        owner_dict["owner"]
        == "COMMONWEALTH OF PENNSYLVANIA; DEPARTMENT OF TRANSPORTATION"
    )

    opa_account_number = "1234"
    maybe_monkeypatch_response(
        monkeypatch, pytestconfig, carto_rows=[],
    )
    recording_date = "2015-06-09"
    owner_dict = rtt_obj.infer_property_ownership(
        opa_account_number=opa_account_number, recording_date=recording_date
    )
    assert owner_dict["owner"] == None