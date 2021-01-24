from abc import ABC, abstractmethod
import pandas as pd
import requests

from .exclusion_filters import CITY_OWNED_EXCLUSION_FILTERS
from .additional_links import (
    get_street_view_link,
    get_atlas_link,
    get_property_phila_gov_link,
)


def remove_city_owned_properties(df):
    for column, values in CITY_OWNED_EXCLUSION_FILTERS.items():
        removal_query = f"{column}.str.strip() not in {tuple(values)}"
        df = df.query(removal_query)
    return df


def _format_opa_account_numbers_sql(opa_account_numbers):
    if type(opa_account_numbers) == list:
        return ", ".join([f"'{num}'" for num in opa_account_numbers])
    else:
        return opa_account_numbers


def _get_query_result(query_str):
    payload = {"q": query_str}
    query_result = requests.get(
        "https://phl.carto.com/api/v2/sql", params=payload
    ).json()
    if "rows" not in query_result:
        raise ValueError(query_str, query_result)
    return query_result


class PhiladelphiaDataTable(ABC):
    def __init__(
        self,
        cartodb_table_name,
        title=None,
        sql_alias=None,
        open_data_philly_table_url_name=None,
    ):
        self.cartodb_table_name = cartodb_table_name
        self.title = title if title else self.name.title()
        self.sql_alias = sql_alias if sql_alias else self.cartodb_table_name[:3]
        self.default_columns = []
        self.dt_column = None
        self.open_data_philly_table_url_name = open_data_philly_table_url_name

        self.default_opa_properties_public_joined_columns = [
            "location",
            "unit",
            "owner_1",
            "owner_2",
            "mailing_care_of",
            "mailing_street",
            "mailing_address_1",
            "mailing_address_2",
            "mailing_city_state",
            "parcel_number",
        ]

    def list(self, limit, where_sql):
        pass

    def query_by_opa_account_numbers(
        self,
        opa_account_numbers,
        dt_column=None,
        columns=None,
        opa_properties_public_joined_columns=None,
    ):
        """
        Parameters
        ----------
        opa_account_numbers: list of str
            Either a SQL query that returns a list of account numbers,
            or a hardcoed list of account numbersto query for in this table
        year_dt_column: str
            SQL column to use to extract a year column from. If None uses the default.
        columns: Can either be a list of columns, or the string 'all' which returns
            all available columns. If nothing is passed, it will use the hardcoded
            default columns.
        opa_properties_public_joined_columns
            Columns to include from the properties table that all tables (other than
            the properties table itself) are joined to. 

        """
        dt_column = dt_column if dt_column else self.dt_column
        columns = columns if columns else self.default_columns
        col_str = self._get_column_sql_from_param(columns)

        opa_properties_public_joined_columns = (
            opa_properties_public_joined_columns
            if opa_properties_public_joined_columns
            else self.default_opa_properties_public_joined_columns
        )
        joined_col_str = self._get_column_sql_from_param(
            opa_properties_public_joined_columns, sql_alias="opa"
        )

        opa_account_numbers = _format_opa_account_numbers_sql(opa_account_numbers)
        sql = self._get_sql_for_query_by_opa_account_numbers(
            opa_account_numbers=opa_account_numbers,
            col_str=col_str,
            joined_col_str=joined_col_str,
        )
        query_result = _get_query_result(sql)
        df = pd.DataFrame(query_result["rows"], columns=query_result["fields"].keys())
        df = df.sort_values(columns, ascending=False)
        if dt_column:
            df.insert(0, "_year", pd.to_datetime(df[dt_column]).dt.year)
        df = remove_city_owned_properties(df)
        return df

    def _get_sql_for_query_by_opa_account_numbers(
        self, opa_account_numbers, col_str, joined_col_str
    ):
        return f"""
        SELECT {col_str}, {joined_col_str}
        FROM {self.cartodb_table_name} {self.sql_alias}
        LEFT JOIN opa_properties_public opa 
            ON {self.sql_alias}.opa_account_num=opa.parcel_number
        WHERE opa.parcel_number in ({opa_account_numbers})
        """

    def _get_column_sql_from_param(self, columns, sql_alias=None):
        sql_alias = sql_alias if sql_alias else self.sql_alias
        if type(columns) == str and columns == "all":
            return f"{self.sql_alias}.*"
        elif type(columns) == list:
            return ", ".join([f"{sql_alias}.{c}" for c in columns])

    def get_odb_link(self):
        return (
            (
                "https://www.opendataphilly.org/dataset/"
                + self.open_data_philly_table_url_name
            )
            if self.open_data_philly_table_url_name
            else None
        )

    def get_cartodb_link(self):
        return (
            "https://cityofphiladelphia.github.io/carto-api-explorer/#"
            + self.cartodb_table_name
        )
