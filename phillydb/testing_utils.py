def maybe_monkeypatch_response(
    monkeypatch, pytestconfig, carto_rows=None, response_override=None
):
    if not pytestconfig.getoption("make_request"):

        def _fake_get(*args, **kwargs):
            if response_override is not None:
                return MockResponse(response_override)
            elif "phl.carto.com" in args[0]:
                return MockCartoResponse(data=carto_rows)
            elif "https://api.phila.gov/tips/account/" in args[0]:
                return MockRealEstateTaxRevenueResponse()
            else:
                return MockResponse()

        monkeypatch.setattr("requests.get", _fake_get)


class MockResponse:
    def __init__(self, data=None, status_code=200):
        self.data = data
        self.status_code = status_code

    def json(self):
        return self.data


class MockRealEstateTaxRevenueResponse(MockResponse):
    def json(self):
        return {
            "data": {
                "accountNum": "01234",
                "property": {"abc": "def"},
                "lastPaymentPostedDate": "2020-01-01",
                "years": [{"def": "ghi", "year": 2020}],
            }
        }


class MockCartoResponse(MockResponse):
    def json(self):
        data = self.data if self.data is not None else [{"opa_account_num": "123"}]
        if self.data is None:
            data = [{"opa_account_num": "123"}]
        elif self.data:
            return {"rows": data, "fields": data[0]}
        else:
            return {"rows": [], "fields": {}}
