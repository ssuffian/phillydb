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
    df = table_obj.list(limit=1).to_dataframe()
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
    df = table_obj.query_by_opa_account_numbers(
        opa_account_numbers=opa_account_numbers
    ).to_dataframe()
    if df.empty:
        raise AssertionError(f"{table_obj.title} failed to return a dataframe.")


def test_all_tables_metadata_urls(table_obj, pytestconfig, monkeypatch):
    maybe_monkeypatch_response(monkeypatch, pytestconfig)
    for link in table_obj.data_links:
        assert requests.get(link).status_code == 200


def test_all_tables_table_get_schema(table_obj, monkeypatch, pytestconfig):
    maybe_monkeypatch_response(
        monkeypatch,
        pytestconfig,
        response_override={
            "records": [
                {
                    "field_17": "abc",
                    "field_188": "Abc",
                    "field_20_raw": "Abc\nDef",
                    "field_20": "Abc<br>Def",
                }
            ]
        },
    )
    if table_obj.cartodb_table_name not in ["condominium", "opa_properties_public_pde"]:
        assert table_obj.get_schema()


def test_query_by_single_str_column(monkeypatch, pytestconfig):
    property_obj = carto_tables.Properties()
    result_columns = ["location", "parcel_number"]
    columns = result_columns
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
    ).to_dataframe()
    assert not df.empty

    license_obj = carto_tables.Licenses()
    result_columns = ["licensetype", "opa_account_num"]
    columns = result_columns

    output_rows = [{c: "2020-01-01 12:00:00" for c in columns}]
    output_rows[0]["opa_account_num"] = "1234"

    maybe_monkeypatch_response(monkeypatch, pytestconfig, output_rows)
    df = license_obj.query_by_single_str_column(
        search_column="licensetype",
        search_to_match="Re",
        search_method="starts with",
        result_columns=result_columns,
        limit=10,
    ).to_dataframe()
    assert not df.empty


def test_guess_property_ownership(monkeypatch, pytestconfig):
    rtt_obj = carto_tables.RealEstateTransfers()
    opa_account_number = "883054500"
    opa_account_number = "881061500"
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


def test_query_with_arcgis(monkeypatch, pytestconfig):
    real_estate = carto_tables.RealEstateTransfers()
    #opa_account_number = "881061500"
    df = real_estate.query_arcgis(
        """
        (((ADDRESS_LOW >= 5427 AND ADDRESS_LOW <= 5427)
        OR (ADDRESS_LOW >= 5400 AND ADDRESS_LOW <= 5427 AND ADDRESS_HIGH >= 27 ))
        AND STREET_NAME = 'WAYNE' AND STREET_SUFFIX = 'AVE' AND (MOD(ADDRESS_LOW,2) = MOD( 5427,2)))
        """
    ).to_dataframe()
    assert not df.empty
