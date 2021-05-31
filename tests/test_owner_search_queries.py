from phillydb.owner_search_queries import OwnerQuery, OwnerQueryResult
from phillydb.carto_tables import Licenses


def test_owner_query():
    owner_query_obj = OwnerQuery("COMMONWEALTH OF PENNSYLVANIA")
    assert owner_query_obj.parcel_num_sql_list

    owner_query_result_obj = OwnerQueryResult(
        owner_query_obj.parcel_num_sql, owner_query_obj.owners_list
    )

    license_obj = Licenses().query_by_opa_account_numbers(
            owner_query_obj.parcel_num_sql
        )
    license_df = license_obj.to_dataframe()
    filtered_license_df = owner_query_result_obj.get_filtered_df(license_df, Licenses().dt_column)
    assert len(license_df) >= len(filtered_license_df)
