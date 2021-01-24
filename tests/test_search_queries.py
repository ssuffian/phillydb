from phillydb import construct_search_query


def test_construct_search_query():
    sql = construct_search_query(
        search_query="LAST FIRST", search_type="mailing address"
    )
    assert "LIKE '%LAST FIRST%'" in sql.upper()

    sql = construct_search_query(
        search_query="LAST FIRST",
        search_type="mailing address",
        search_method="starts with",
    )
    assert "LIKE 'LAST FIRST%'" in sql.upper()

    sql = construct_search_query(
        search_query="LAST FIRST",
        search_type="mailing address",
        search_method="ends with",
    )
    assert "LIKE '%LAST FIRST'" in sql.upper()
