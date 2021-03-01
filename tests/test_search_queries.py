import pytest

from phillydb import construct_search_query
from phillydb.exceptions import (
    SearchMethodNotImplementedError,
    SearchTypeNotImplementedError,
)


def test_construct_search_query():
    sql = construct_search_query(
        search_to_match="LAST FIRST", search_type="mailing_address"
    )
    assert "LIKE '%LAST FIRST%'" in sql.upper()

    sql = construct_search_query(
        search_to_match="LAST FIRST", search_type="owner", search_method="starts with",
    )
    assert "LIKE 'LAST FIRST%'" in sql.upper()

    sql = construct_search_query(
        search_to_match="LAST FIRST",
        search_type="location_by_owner",
        search_method="ends with",
    )
    assert "LIKE '%LAST FIRST'" in sql.upper()

    sql = construct_search_query(
        search_to_match="LAST FIRST",
        search_type="location_by_mailing_address",
        search_method="ends with",
    )
    assert "LIKE '%LAST FIRST'" in sql.upper()

    with pytest.raises(SearchMethodNotImplementedError):
        construct_search_query(
            search_to_match="LAST FIRST",
            search_type="mailing_address",
            search_method="abcd",
        )

    with pytest.raises(SearchTypeNotImplementedError):
        construct_search_query(
            search_to_match="LAST FIRST", search_type="abc", search_method="contains",
        )
