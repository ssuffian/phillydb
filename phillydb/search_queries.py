from abc import ABC
from .exceptions import (
    SearchMethodNotImplementedError,
    SearchTypeNotImplementedError,
)


def search_method_sql(search_to_match, search_method):
    if search_method == "contains":
        return f"%{search_to_match}%"
    elif search_method == "starts with":
        return f"{search_to_match}%"
    elif search_method == "ends with":
        return f"%{search_to_match}"
    elif search_method == "equals":
        return search_to_match
    else:
        raise SearchMethodNotImplementedError(search_method)


def construct_search_query(
    search_to_match,
    search_type,
    search_method="contains",
):
    """
    Takes in a search query and method of searching, and constructs a SQL subquery
    that when executed provides a list of opa account numbers.
    search_method=['contains', 'starts with', 'ends with', 'equals'],

    Parameters
    ----------
    search_to_match: str
    search_type: str
        One of: ['mailing_address', 'owner', 'location_by_owner',
            'location_by_mailing_address', 'parcel_number'
            ]
    search_method: str
        One of: ['contains', 'starts with', 'ends with', 'equals']
    """
    search_to_match = search_method_sql(search_to_match.upper(), search_method)
    if search_type == "parcel_number":
        opa_account_numbers_sql = f"""
            SELECT opp.parcel_number FROM opa_properties_public opp 
                WHERE (
                    opp.parcel_number LIKE '{search_to_match}'
                )
        """
    elif search_type == "owner":
        opa_account_numbers_sql = f"""
            SELECT opp.parcel_number FROM opa_properties_public opp 
                WHERE (
                    owner_1 LIKE '{search_to_match}' OR owner_2 LIKE '{search_to_match}'
                )
            UNION ALL
            SELECT opa_account_num FROM business_licenses bl, opa_properties_public opp
                WHERE bl.opa_account_num = opp.parcel_number
                AND licensetype = 'Rental'
                AND (
                    legalname LIKE '{search_to_match}' OR business_name LIKE '{search_to_match}'
                )
        """
    elif search_type == "mailing_address":
        opa_account_numbers_sql = f"""
        SELECT opp.parcel_number FROM opa_properties_public opp
            WHERE (
                mailing_address_1 LIKE '{search_to_match}' 
                OR mailing_street LIKE '{search_to_match}'
            )
        """
    elif search_type == "location_by_owner":
        opa_account_numbers_sql = f"""
         SELECT opp2.parcel_number FROM opa_properties_public opp1, opa_properties_public opp2 
            WHERE opp1.location LIKE '{search_to_match}' AND (
                opp1.owner_1 = opp2.owner_1 OR opp1.owner_2 = opp2.owner_2
                OR opp1.owner_1 = opp2.owner_2 OR opp1.owner_2 = opp2.owner_1
            )
        """
    elif search_type == "location_by_mailing_address":
        opa_account_numbers_sql = f"""
         SELECT opp2.parcel_number FROM opa_properties_public opp1, opa_properties_public opp2 
            WHERE opp1.location LIKE '{search_to_match}' AND ( 
                opp1.mailing_street = opp2.mailing_street OR (
                    opp1.mailing_address_1 = opp2.mailing_address_1 
                    AND opp1.mailing_address_2 = opp2.mailing_address_2
                )
            )
        """
    else:
        raise SearchTypeNotImplementedError(search_type)

    return opa_account_numbers_sql
