
def maybe_monkeypatch_response(monkeypatch, pytestconfig, columns=None):
    if not pytestconfig.getoption("make_request"):

        def _fake_get(*args, **kwargs):
            if "phl.carto.com" in args[0]:
                return MockCartoResponse(columns=columns)
            else:
                return MockRealEstateTaxRevenueResponse()

        monkeypatch.setattr("requests.get", _fake_get)


class MockRealEstateTaxRevenueResponse:
    def __init__(self):
        self.status_code = 200
        pass

    def json(self):
        return {
            "data": {
                "accountNum": "01234",
                "property": {"abc": "def"},
                "lastPaymentPostedDate": "2020-01-01",
                "years": [{"def": "ghi", "year": 2020}],
            }
        }


class MockCartoResponse:
    def __init__(self, columns=None):
        self.columns = columns if columns else []
        self.status_code = 200
        pass

    def json(self):
        columns_dict = {c: "2020-01-01 12:00:00" for c in self.columns}
        if 'parcel_number' in self.columns:
            columns_dict['parcel_number'] = '1234'
        elif 'opa_account_num' in self.columns:
            columns_dict['opa_account_num'] = '1234'
        return {"rows": [columns_dict], "fields": columns_dict}

