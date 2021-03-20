from phillydb.tables import RealEstateTaxRevenue
from phillydb.testing_utils import maybe_monkeypatch_response


def test_real_estate_tax_revenue(opa_account_numbers, monkeypatch, pytestconfig):
    tx_rv = RealEstateTaxRevenue()
    maybe_monkeypatch_response(
        monkeypatch, pytestconfig,
        ['parcel_number']
    )
    df = tx_rv.query_by_opa_account_numbers(opa_account_numbers)
    assert not df.empty
