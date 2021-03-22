import pandas as pd
import pytest

from phillydb.tables import (
    RealEstateTaxRevenue,
    remove_city_owned_properties,
    get_query_result,
    get_query_result_df,
)
from phillydb.testing_utils import maybe_monkeypatch_response


def test_real_estate_tax_revenue(opa_account_numbers, monkeypatch, pytestconfig):
    tx_rv = RealEstateTaxRevenue()
    if not pytestconfig.getoption("make_request"):
        # remove the wait if monkeypatching the request
        tx_rv.rate_limit_wait_secs = 0
    output_rows = [{c: "2020-01-01 12:00:00" for c in ["parcel_number"]}]
    maybe_monkeypatch_response(monkeypatch, pytestconfig, output_rows)
    df = tx_rv.query_by_opa_account_numbers(opa_account_numbers)
    assert not df.empty


def test_remove_city_owned_properties():
    with pytest.raises(ValueError):
        remove_city_owned_properties(pd.DataFrame())


def test_get_query_result(monkeypatch, pytestconfig):
    maybe_monkeypatch_response(monkeypatch, pytestconfig,  response_override={})
    with pytest.raises(ValueError):
        rows = get_query_result("SELECTFROMSQLTABLESYNTAXERROR")
