from abc import ABC


def construct_search_query(
    search_query, search_type, search_method="contains",
):
    """
    Takes in a search query and method of searching, and constructs a SQL subquery
    that when executed provides a list of opa account numbers.
    search_method=['contains', 'starts with', 'ends with', 'equals'],

    Parameters
    ----------
    search_query: str
    search_method: str
        One of: ['contains', 'starts with', 'ends with', 'equals']
    search_type: str
        One of: ['mailing address', 'owner', 'address (and others w/ same owner)',
            'address (and others w/ same mailing address)'
            ]
    """
    print("Loading data...")
    search_query = search_query.upper()
    if search_method == "contains":
        search_query = f"%{search_query}%"
    elif search_method == "starts with":
        search_query = f"{search_query}%"
    elif search_method == "ends with":
        search_query = f"%{search_query}"
    elif search_method == "equals":
        search_query = search_query
    else:
        raise ValueError(f"Can not currently support search_method: {search_method}")

    if search_type == "owner":
        opa_account_numbers_sql = f"""
            SELECT opp.parcel_number FROM opa_properties_public opp 
                WHERE (
                    owner_1 LIKE '{search_query}' OR owner_2 LIKE '{search_query}'
                )
            UNION ALL
            SELECT opa_account_num FROM business_licenses bl, opa_properties_public opp
                WHERE bl.opa_account_num = opp.parcel_number
                AND licensetype = 'Rental' and licensestatus = 'Active'
                AND (
                    legalname LIKE '{search_query}' OR business_name LIKE '{search_query}'
                )
        """
    elif search_type == "mailing address":
        opa_account_numbers_sql = f"""
        SELECT opp.parcel_number FROM opa_properties_public opp
            WHERE (
                mailing_address_1 LIKE '{search_query}' 
                OR mailing_street LIKE '{search_query}'
            )
        """
    elif search_type == "address (and others w/ same owner)":
        opa_account_numbers_sql = f"""
         SELECT opp2.parcel_number FROM opa_properties_public opp1, opa_properties_public opp2 
            WHERE opp1.location LIKE '{search_query}' AND (
                opp1.owner_1 = opp2.owner_1 OR opp1.owner_2 = opp2.owner_2
                OR opp1.owner_1 = opp2.owner_2 OR opp1.owner_2 = opp2.owner_1
            )
        """
    elif search_type == "address (and others w/ same mailing address)":
        opa_account_numbers_sql = f"""
         SELECT opp2.parcel_number FROM opa_properties_public opp1, opa_properties_public opp2 
            WHERE opp1.location LIKE '{search_query}' AND ( 
                opp1.mailing_street = opp2.mailing_street OR (
                    opp1.mailing_address_1 = opp2.mailing_address_1 
                    AND opp1.mailing_address_2 = opp2.mailing_address_2
                )
            )
        """
    else:
        raise ValueError(f"Can not currently support search_type: {search_type}")

    return opa_account_numbers_sql


"""
class SearchMethod(ABC):
    def __init__(self, name, description):
        self.name = name
        self.description = description


class SearchMethodContains(SearchMethod):
    return super().__init("contains", "contains")


class SearchMethodStartsWith(SearchMethod):
    return super().__init("starts_with", "starts with")


class SearchMethodEndsWith(SearchMethod):
    return super().__init("ends_with", "ends with")


class SearchType(ABC):
    def __init__(self, name, description):
        self.name = name
        self.description = description


class SearchTypeOwner(SearchType):
    return super().__init__("owner", "owner")


class SearchTypeMailingAddress(SearchType):
    return super().__init__("mailing_address", "mailing address")


class SearchTypeAddressByOwner(SearchType):
    return super().__init__("address_by_owner", "address (and others w/ same owner)")


class SearchTypeAddressByMailingAddress(SearchType):
    return super().__init__(
        "address_by_mailing_address", "address (and others w/ same mailing address)"
    )
"""
