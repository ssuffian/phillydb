from abc import ABC, abstractmethod
from datetime import datetime
import logging
import pandas as pd
import requests
import time
import sys
import urllib

from .labelled_fields import label_city_owned_properties, CITY_OWNED_PROPERTY_FIELDS
from .additional_links import (
    get_street_view_link,
    get_atlas_link,
    get_property_phila_gov_link,
    get_license_inspections_link,
)
from .search_queries import search_method_sql


logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)


def _format_opa_account_numbers_sql(opa_account_numbers):
    if type(opa_account_numbers) == list:
        return ", ".join([f"'{num}'" for num in opa_account_numbers])
    else:
        return opa_account_numbers


class PhillyArcgisQuery(ABC):
    """Query the arcgis server that contains raw open philly data

    Notes
    -----
    - It seems to only return a max of 2000 entries per query.
    """

    def __init__(self, where_sql, table):
        self.where_sql = where_sql
        self.table = table

    def execute(self):
        # out_fields = ",+".join(self.columns + ['opa_account_num']).upper()
        # going to return all out fields for now
        out_fields = "*"
        encoded_sql = urllib.parse.quote_plus(self.where_sql)
        request_url = (
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services/"
            f"{self.table}/FeatureServer/0/query?returnDistinctValues=true"
            f"&returnGeometry=false&f=json&sqlFormat=standard&where={self.where_sql}"
            f"&outFields={out_fields}"
        )
        query_result = requests.get(request_url)
        return PhillyArcgisQueryResult(
            sql=self.where_sql,
            table=self.table,
            query_result=query_result,
            remove_all_city_owned_properties=False,
            validation_field_to_check="features",
        )


class PhillyCartoQuery(ABC):
    """Query the Carto database (modeled after ibis)"""

    def __init__(self, sql):
        self.sql = sql

    def execute(self, remove_all_city_owned_properties=False):
        payload = {"q": self.sql}
        query_result = requests.get("https://phl.carto.com/api/v2/sql", params=payload)
        return PhillyCartoQueryResult(
            sql=self.sql,
            query_result=query_result,
            remove_all_city_owned_properties=remove_all_city_owned_properties,
        )


class PhillyQueryResult(ABC):
    def __init__(
        self,
        query_result,
        remove_all_city_owned_properties=True,
        validation_field_to_check="rows",
        **kwargs,
    ):
        self.kwargs = kwargs
        self.sql = kwargs.get("sql")
        self.query_result = query_result
        self.results_json = self.query_result.json()

        self.remove_all_city_owned_properties = remove_all_city_owned_properties

        if validation_field_to_check not in self.results_json:
            raise ValueError(self.sql, self.results_json)

    def to_json(self):
        return self.results_json

    def _get_dataframe(self, *args, **kwargs):
        raise NotImplementedError

    def to_dataframe(self, dt_column=None):
        df = self._get_dataframe()
        columns = df.columns.values.tolist()
        df = df if df.empty else df.sort_values(columns, ascending=False)

        # adds a year column based on the specified datetime column
        if dt_column:
            df.insert(0, "dt_year", pd.to_datetime(df[dt_column]).dt.year)

        # adds hyperlinks to city websites
        if "location" in columns:
            df["link_cyclomedia_street_view"] = df["location"].apply(
                get_street_view_link
            )
            df["link_license_inspections"] = df["location"].apply(
                get_license_inspections_link
            )
        for opa_account_col in ["opa_account_num", "parcel_number"]:
            if opa_account_col in columns:
                df["link_property_phila_gov"] = df[opa_account_col].apply(
                    get_property_phila_gov_link
                )
                df["link_atlas"] = df[opa_account_col].apply(get_atlas_link)
                break

        """
        # No longer removing city-owned properties
        df = label_city_owned_properties(
            df, remove=self.remove_all_city_owned_properties
        )
        """
        return df


class PhillyCartoQueryResult(PhillyQueryResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_dataframe(self):
        columns = list(self.results_json["fields"].keys())
        return pd.DataFrame(self.results_json["rows"], columns=columns)


class PhillyArcgisQueryResult(PhillyQueryResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_dataframe(self):
        df = pd.DataFrame([r["attributes"] for r in self.results_json["features"]])
        date_cols = [
            r["name"]
            for r in self.results_json["fields"]
            if r["type"] == "esriFieldTypeDate"
        ]
        for col in date_cols:
            df[col] = df[col].apply(
                lambda x: datetime.fromtimestamp(x / 1000.0) if pd.notnull(x) else x
            )
        return df


class PhillyCartoTable(ABC):
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
        self.arcgis_table_name = cartodb_table_name.upper()
        self.title = title if title else self.name.title()
        self.sql_alias = sql_alias if sql_alias else self.cartodb_table_name[:3]
        self.default_columns = []  # set by subclass
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
            "year_built",
        ]
        self.city_owned_prop_cols = list(CITY_OWNED_PROPERTY_FIELDS.keys())

    def __str__(self):
        return f"{self.title}, {self.cartodb_table_name}"

    def list(
        self,
        columns=None,
        where_sql=None,
        order_by_columns=None,
        limit=None,
        offset=None,
    ):
        select_sql = ",".join(columns) if columns else "*"
        where_sql = f"WHERE {where_sql}" if where_sql else ""
        offset_sql = f"OFFSET {offset}" if offset else ""
        limit_sql = f"LIMIT {limit}" if limit else ""
        order_by_sql = (
            "ORDER BY " + ",".join(order_by_columns) if order_by_columns else ""
        )
        return PhillyCartoQuery(
            f"""
            SELECT {select_sql}
            FROM {self.cartodb_table_name} {self.sql_alias}
            {where_sql}
            {limit_sql}
            {offset_sql}
            {order_by_sql}
            """
        ).execute(remove_all_city_owned_properties=False)

    def query_arcgis(
        self,
        where_sql,
        columns=None,
    ):
        return PhillyArcgisQuery(
            where_sql=where_sql,
            table=self.arcgis_table_name,
        ).execute()

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
        dt_column: str
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
        return PhillyCartoQuery(sql).execute(
            remove_all_city_owned_properties=remove_all_city_owned_properties
        )

    def query_by_single_str_column(
        self,
        search_column,
        search_to_match,
        search_method="starts with",
        result_columns=None,
        limit=None,
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
            self.city_owned_prop_cols, sql_alias="opa"
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
        return PhillyCartoQuery(sql).execute()

    def _get_sql_for_query_by_single_str_column(
        self,
        search_column,
        col_str,
        joined_col_str,
        search_to_match,
        limit_str,
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
        self,
        opa_account_numbers,
        remove_all_city_owned_properties=True,
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
                for r in PhillyCartoQuery(opa_account_numbers_sql)
                .execute()
                .to_json()["rows"]
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
