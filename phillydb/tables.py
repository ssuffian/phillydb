from abc import ABC, abstractmethod
import logging
import pandas as pd
import requests
import time

from .exclusion_filters import CITY_OWNED_EXCLUSION_FILTERS
from .additional_links import (
    get_street_view_link,
    get_atlas_link,
    get_property_phila_gov_link,
    get_license_inspections_link,
)
from .search_queries import search_method_sql
from .log_config import logger


def remove_city_owned_properties(df):
    city_owned_required_cols = CITY_OWNED_EXCLUSION_FILTERS.keys()
    for column, values in CITY_OWNED_EXCLUSION_FILTERS.items():
        if column not in df.columns:
            raise ValueError(
                f"{column} not in results. You must "
                f"request {city_owned_required_cols} columns "
                "in order to remove city-owned properties."
            )
        removal_query = f"{column}.str.strip() not in {tuple(values)}"
        df = df.query(removal_query)
    return df


def _format_opa_account_numbers_sql(opa_account_numbers):
    if type(opa_account_numbers) == list:
        return ", ".join([f"'{num}'" for num in opa_account_numbers])
    else:
        return opa_account_numbers


def get_query_result(query_str):
    payload = {"q": query_str}
    query_result = requests.get(
        "https://phl.carto.com/api/v2/sql", params=payload
    ).json()
    if "rows" not in query_result:
        raise ValueError(query_str, query_result)
    return query_result


def get_query_result_df(sql, remove_all_city_owned_properties=True, dt_column=None):
    query_result = get_query_result(sql)
    columns = list(query_result["fields"].keys())
    df = pd.DataFrame(query_result["rows"], columns=columns)
    if columns:
        df = df.sort_values(columns, ascending=False)

    # adds a year column based on the specified datetime column
    if dt_column:
        df.insert(0, "dt_year", pd.to_datetime(df[dt_column]).dt.year)

    # adds hyperlinks to city websites
    if "location" in columns:
        df["link_cyclomedia_street_view"] = df["location"].apply(get_street_view_link)
    for opa_account_col in ["opa_account_num", "parcel_number"]:
        if opa_account_col in columns:
            df["link_property_phila_gov"] = df[opa_account_col].apply(
                get_property_phila_gov_link
            )
            df["link_atlas"] = df[opa_account_col].apply(get_atlas_link)
            df["link_license_inspections"] = df[opa_account_col].apply(
                get_license_inspections_link
            )
            break

    if remove_all_city_owned_properties:
        df = remove_city_owned_properties(df)
    return df


class PhiladelphiaCartoDataTable(ABC):
    def __init__(
        self,
        cartodb_table_name,
        title=None,
        sql_alias=None,
        open_data_philly_table_url_name=None,
        schema_application_id=None,
        schema_representation_id=None,
    ):
        self.cartodb_table_name = cartodb_table_name
        self.title = title if title else self.name.title()
        self.sql_alias = sql_alias if sql_alias else self.cartodb_table_name[:3]
        self.default_columns = []
        self.dt_column = None
        self.open_data_philly_table_url_name = open_data_philly_table_url_name
        self.schema_application_id = schema_application_id
        self.schema_representation_id = schema_representation_id

        self.data_links = self._get_data_links()

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
        self.city_owned_prop_filter_cols = list(CITY_OWNED_EXCLUSION_FILTERS.keys())

    def __str__(self):
        return f'{self.title}, {self.cartodb_table_name}'

    def list(
        self,
        columns=None,
        where_sql=None,
        order_by_columns=None,
        limit=None,
        offset=None,
        remove_all_city_owned_properties=False,
    ):
        select_sql = ",".join(columns) if columns else "*"
        where_sql = f"WHERE {where_sql}" if where_sql else ""
        offset_sql = f"OFFSET {offset}" if offset else ""
        limit_sql = f"LIMIT {limit}" if limit else ""
        order_by_sql = (
            "ORDER BY " + ",".join(order_by_columns) if order_by_columns else ""
        )
        return get_query_result_df(
            f"""
            SELECT {select_sql}
            FROM {self.cartodb_table_name} {self.sql_alias}
            {where_sql}
            {limit_sql}
            {offset_sql}
            {order_by_sql}
            """,
            remove_all_city_owned_properties=remove_all_city_owned_properties,
        )

    def query_by_opa_account_numbers(
        self,
        opa_account_numbers,
        dt_column=None,
        columns=None,
        opa_properties_public_joined_columns=None,
        remove_all_city_owned_properties=True,
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
        return get_query_result_df(
            sql,
            dt_column=dt_column,
            remove_all_city_owned_properties=remove_all_city_owned_properties,
        )

    def query_by_single_str_column(
        self,
        search_column,
        search_to_match,
        search_method="starts with",
        result_columns=None,
        limit=None,
        remove_all_city_owned_properties=True,
    ):
        """
        A more general way to get results by a single column string match.
        This is helpful for autocomplete functionality.

        Parameters
        ----------
        search_column: str
            Column to use for querying
        search_to_match:
            Value to use for querying to match search_column
        search_method: str
            One of: ['contains', 'starts with', 'ends with', 'equals']
        result_columns:
            Other columns to output when executing this query
        """
        result_columns = result_columns if result_columns else self.default_columns
        col_str = self._get_column_sql_from_param(result_columns)
        joined_col_str = self._get_column_sql_from_param(
            self.city_owned_prop_filter_cols, sql_alias="opa"
        )
        search_to_match = search_method_sql(search_to_match, search_method)
        limit_str = f"LIMIT {limit}" if limit else ""
        sql = self._get_sql_for_query_by_single_str_column(
            search_column=search_column,
            col_str=col_str,
            joined_col_str=joined_col_str,
            search_to_match=search_to_match,
            limit_str=limit_str,
        )
        return get_query_result_df(
            sql, remove_all_city_owned_properties=remove_all_city_owned_properties,
        )

    def _get_sql_for_query_by_single_str_column(
        self, search_column, col_str, joined_col_str, search_to_match, limit_str,
    ):
        return f"""
        SELECT {col_str}, {joined_col_str}
        FROM {self.cartodb_table_name} {self.sql_alias}
        LEFT JOIN opa_properties_public opa 
            ON {self.sql_alias}.opa_account_num=opa.parcel_number
        WHERE {self.sql_alias}.{search_column} LIKE '{search_to_match}' 
        {limit_str}
        """

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

    def _get_data_links(self):
        data_links = []
        odb_link = self._get_odb_link()
        if odb_link:
            data_links.append(odb_link)
        cartodb_link = self._get_cartodb_link()
        if cartodb_link:
            data_links.append(cartodb_link)
        schema_link = self._get_schema_link()
        if schema_link:
            data_links.append(schema_link)
        return data_links

    def _get_odb_link(self):
        return (
            (
                "https://www.opendataphilly.org/dataset/"
                + self.open_data_philly_table_url_name
            )
            if self.open_data_philly_table_url_name
            else None
        )

    def _get_schema_link(self, as_json=False):
        if self.schema_representation_id and self.schema_application_id:
            if as_json:
                return (
                    "https://us-api.knack.com/v1/scenes/scene_142/views/view_287/records/"
                    f"export/applications/550c60d00711ffe12e9efc64?type=json"
                    f"&representationdetails_id={self.schema_representation_id}"
                )
            else:
                return (
                    "https://schema.phila.gov/#home/datasetdetails/"
                    f"{self.schema_application_id}/representationdetails/"
                    f"{self.schema_representation_id}/"
                )

    def _get_cartodb_link(self):
        return (
            "https://cityofphiladelphia.github.io/carto-api-explorer/#"
            + self.cartodb_table_name
        )

    def get_schema(self):
        schema_link = self._get_schema_link(as_json=True)
        if schema_link:
            response = requests.get(schema_link)
            return [
                {
                    "column": r["field_17"],
                    "column_for_display": r["field_188"],
                    "description_str": r["field_20_raw"],
                    "description_html": r["field_20"],
                }
                for r in response.json()["records"]
            ]


class RealEstateTaxRevenue(ABC):
    def __init__(self, rate_limit_wait_secs=5):
        self.data_links = [
            "https://www.phila.gov/revenue/realestatetax/",
            "https://github.com/CityOfPhiladelphia/tips-api",
        ]
        self.rate_limit_wait_secs = rate_limit_wait_secs

    def query_by_opa_account_numbers(
        self, opa_account_numbers, remove_all_city_owned_properties=True,
    ):
        """
        Parameters
        ----------
        """
        if type(opa_account_numbers) == list:
            opa_account_num_list = opa_account_numbers
        else:
            opa_account_numbers_sql = _format_opa_account_numbers_sql(
                opa_account_numbers
            )
            opa_account_num_list = [
                r["parcel_number"]
                for r in get_query_result(opa_account_numbers_sql)["rows"]
            ]
        output_dfs = []
        for account_num in opa_account_num_list:
            url = f"https://api.phila.gov/tips/account/{account_num}"
            response = requests.get(url).json()
            logger.info(
                f"Waiting {self.rate_limit_wait_secs} seconds to not violate rate limit."
            )
            time.sleep(self.rate_limit_wait_secs)
            if "data" in response:
                data = response["data"]
                property_data = data["property"]
                years_data = data["years"]
                df = pd.DataFrame(years_data)
                df["last_payment_posted_date"] = data["lastPaymentPostedDate"]
                df["account_num"] = data["accountNum"]
                for key, val in property_data.items():
                    df[key] = val
                output_dfs.append(df)
        return pd.concat(output_dfs)
