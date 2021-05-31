import pandas as pd

from .tables import PhillyCartoTable


__all__ = (
    "Properties",
    "Permits",
    "Licenses",
    "Violations",
    "Condominiums",
    "Complaints",
    "Appeals",
    "RealEstateTaxDelinquencies",
    "RealEstateTransfers",
    "CaseInvestigations",
    "PropertiesPde",
)


class Properties(PhillyCartoTable):
    def __init__(self, title="Properties"):
        """
        This query always returns latitude and longitude in addition to any other
        columns requested.
        """
        super().__init__(
            cartodb_table_name="opa_properties_public",
            title=title,
            sql_alias="opa",  # other pieces of code rely on this to be 'opa'
            open_data_philly_table_url_name="opa-property-assessments",
            schema_application_id="550c60d00711ffe12e9efc64",
            schema_representation_id="55d624fdad35c7e854cb21a4",
        )
        self.default_columns = [
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
            "building_code_description",
            "category_code_description",
            "homestead_exemption",
            "year_built",
            "year_built_estimate",
        ]

    def _get_sql_for_query_by_opa_account_numbers(
        self, opa_account_numbers, col_str, joined_col_str
    ):
        # no joining necessary since this is the primary table
        return f"""
        SELECT  {col_str}, ST_Y(the_geom) AS lat, ST_X(the_geom) AS lng
        FROM {self.cartodb_table_name} {self.sql_alias}
        WHERE parcel_number in ({opa_account_numbers})
        """

    def _get_sql_for_query_by_single_str_column(
        self,
        col_str,
        joined_col_str,
        search_column,
        search_to_match,
        limit_str,
    ):
        return f"""
        SELECT {col_str}, {joined_col_str}
        FROM {self.cartodb_table_name} {self.sql_alias}
        WHERE {self.sql_alias}.{search_column} LIKE '{search_to_match}' 
        {limit_str}
        """


class Licenses(PhillyCartoTable):
    def __init__(self, title="Licenses"):
        super().__init__(
            cartodb_table_name="business_licenses",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-business-licenses",
            schema_application_id="5543ca6f5c4ae4cd66d3ff59",
            schema_representation_id="5e9a06fb98cc42001606f331",
        )
        self.default_columns = [
            "mostrecentissuedate",
            "licensestatus",
            "licensetype",
            "legalname",
            "business_name",
            "opa_owner",
            "numberofunits",
        ]
        self.dt_column = "mostrecentissuedate"


class Condominiums(PhillyCartoTable):
    def __init__(self, title="Condominiums"):
        super().__init__(
            cartodb_table_name="condominium",
            title=title,
        )
        self.default_columns = [
            "condo_name",
            "condounit",
            "orig_date",
            "inactdate",
            "status",
        ]

    def _get_sql_for_query_by_opa_account_numbers(
        self, opa_account_numbers, col_str, joined_col_str
    ):
        # involves an extra intermediary join through the rtt_summary
        return f"""
        SELECT {col_str}, {joined_col_str}
        FROM {self.cartodb_table_name} {self.sql_alias}
        LEFT JOIN rtt_summary rtt ON {self.sql_alias}.mapref=rtt.matched_regmap
        LEFT JOIN opa_properties_public opa ON rtt.opa_account_num=opa.parcel_number
        WHERE opa.parcel_number IN ({opa_account_numbers})
            AND rtt.unit_num = {self.sql_alias}.condounit
        """


class Complaints(PhillyCartoTable):
    def __init__(self, title="Complaints"):
        super().__init__(
            cartodb_table_name="complaints",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-service-requests",
            schema_application_id="5543ca6d5c4ae4cd66d3ff52",
            schema_representation_id="5e5d50e0fbc9650019b56025",
        )
        self.default_columns = [
            "complaintdate",
            "complaintnumber",
            "complaintcodename",
            "complaintdate",
            "complaintstatus",
            "casestatus",
            "initialinvestigation_date",
            "complaintresolution_date",
        ]
        self.dt_column = "complaintdate"


class Violations(PhillyCartoTable):
    def __init__(self, title="Violations"):
        super().__init__(
            cartodb_table_name="violations",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-violations",
            schema_application_id="5543ca7a5c4ae4cd66d3ff86",
            schema_representation_id="5e99bab227c80700158695b0",
        )
        self.default_columns = [
            "violationdate",
            "caseprioritydesc",
            "violationcode",
            "violationcodetitle",
        ]
        self.dt_column = "violationdate"


class Permits(PhillyCartoTable):
    def __init__(self, title="Permits"):
        super().__init__(
            cartodb_table_name="permits",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-building-permits",
            schema_application_id="5543868920583086178c4f8f",
            schema_representation_id="5e9a01ac801624001585ca11",
        )
        self.default_columns = [
            "permitissuedate",
            "permitnumber",
            "permitdescription",
            "typeofwork",
            "approvedscopeofwork",
            "status",
            "applicanttype",
            "contractorname",
            "contractoraddress1",
            "contractoraddress2",
            "contractorcity",
            "contractorstate",
            "contractorzip",
            "mostrecentinsp",
        ]
        self.dt_column = "permitissuedate"


class Appeals(PhillyCartoTable):
    def __init__(self, title="Appeals"):
        super().__init__(
            cartodb_table_name="appeals",
            title=title,
            open_data_philly_table_url_name="license-and-inspections-appeals",
            schema_application_id="5543864d20583086178c4e9c",
            schema_representation_id="5e9751361ed3930016d62645",
        )
        self.default_columns = [
            "createddate",
            "primaryappellant",
            "appellanttype",
            "appealnumber",
            "acceleratedappeal",
            "completeddate",
            "relatedpermit",
            "relatedcasefile",
            "appealstatus",
            "appealtype",
            "agendadescription",
            "applicationtype",
            "meetingnumber",
            "appealgrounds",
            "scheduleddate",
            "decision",
            "meetingresult",
            "proviso",
            "decisiondate",
        ]

        self.dt_column = "createddate"


class RealEstateTaxDelinquencies(PhillyCartoTable):
    def __init__(self, title="Real Estate Tax Delinquencies"):
        super().__init__(
            cartodb_table_name="real_estate_tax_delinquencies",
            title=title,
            open_data_philly_table_url_name="property-tax-delinquencies",
            schema_application_id="57d9643afab162fe2708224e",
            schema_representation_id="57d9643cfab162fe27082252",
        )
        self.default_columns = [
            "most_recent_year_owed",
            "street_address",
            "total_due",
            "num_years_owed",
            "most_recent_payment_date",
            "coll_agency_num_years",
            "years_in_bankruptcy",
            "building_category",
        ]

        self.dt_column = "most_recent_year_owed"

    def _get_sql_for_query_by_opa_account_numbers(
        self, opa_account_numbers, col_str, joined_col_str
    ):
        # involves casting opa_number to text
        return f"""
        SELECT {col_str}, {joined_col_str}
        FROM {self.cartodb_table_name} {self.sql_alias}
        LEFT JOIN opa_properties_public opa 
            ON cast({self.sql_alias}.opa_number as text)=opa.parcel_number
        WHERE opa.parcel_number in ({opa_account_numbers})
        """


class RealEstateTransfers(PhillyCartoTable):
    def __init__(self, title="Real Estate Transfers"):
        super().__init__(
            cartodb_table_name="rtt_summary",
            title=title,
            open_data_philly_table_url_name="real-estate-transfers",
            schema_application_id="5a04b8d39202605970a7457d",
            schema_representation_id="5a04b8d39202605970a74581",
        )
        self.default_columns = [
            "receipt_date",
            "street_address",
            "grantors",
            "grantees",
            "total_consideration",
            "condo_name",
            "unit_num",
            "receipt_date",
            "recording_date",
            "document_id",
            "document_type",
        ]

        self.dt_column = "receiptdate"

    def infer_property_ownership(self, opa_account_number, recording_date=None):
        """Infer property ownership at a specific point in time

        Based on the owner of the deed at the time, provides the owner name (via the
        grantee field) for a given date at a given property.

        This is done using the following rules:

        1) Search for all of the DEED transactions that took place at a property
        2) reconstruct the address from the address components
        3) compare this reconstructed address with the provided `street_address` col
            3a) This is because we have found DEEDs that were associated with
                an opa_account_num but didn't turn out to be actually for that
                property.
        4) Look at the recording_date provided (or use datetime.now() if None provided)
        5) Find the most recent DEED grantee before the given recording_date
            5a) If None is found (the rtt_summary data doesn't start until 1999),
                then use the earliest grantor in the DEED data provided.

        This function introduces the concept of 'discrepencies' which may be
        expanded elsewhere in this library. These are assumptions/estimates/deviations
        that were necessary to return a result, but may not be as reliable as otherwise
        standard results. 3a and 5a above are examples of discrepencies.

        Parameters
        ----------
        opa_account_num: str
            The opa_account_num for the property.
        recording_date: str
            The date to look for property ownership.
        """

        recording_date = (
            recording_date if recording_date else datetime.now().isoformat()
        )
        list_result = self.list(
            columns=[
                "opa_account_num",
                "recording_date",
                "grantors",
                "grantees",
                "address_low",
                "address_low_suffix",
                "address_high",
                "address_low_frac",
                "street_predir",
                "street_name",
                "street_suffix",
                "street_address",
            ],
            where_sql=f"""
            (
                document_type='DEED' OR
                document_type='DEED SHERIFF' OR
                document_type='DEED OF CONDEMNATION' OR
                document_type='DEED LAND BANK'
            ) AND opa_account_num ='{opa_account_number}'
            """,
            order_by_columns=["recording_date"],
        )
        df = list_result.to_dataframe()
        if df.empty:
            df_location = (
                Properties()
                .query_by_opa_account_numbers([opa_account_number])
                .to_dataframe()
            )
            if not df_location.empty:
                location_address = df_location.iloc[0]["location"]

        """
        (((ADDRESS_LOW >= 5427 AND ADDRESS_LOW <= 5427)
        OR (ADDRESS_LOW >= 5400 AND ADDRESS_LOW <= 5427 AND ADDRESS_HIGH >= 27 ))
        AND STREET_NAME = 'WAYNE' AND STREET_SUFFIX = 'AVE' AND (MOD(ADDRESS_LOW,2) = MOD( 5427,2)))
        """
        return self.infer_property_ownership_from_df(
            df, opa_account_number, recording_date
        )

    def infer_property_ownership_from_df(self, df, opa_account_number, recording_date):
        def _compose_address(x):
            def _strfy(col, prefix=""):
                return prefix + str(x.loc[col]) if x.loc[col] else ""

            address_low = _strfy("address_low")
            address_low_suffix = _strfy("address_low_suffix")
            address_high = _strfy("address_high", prefix="-")
            address_low_frac = _strfy("address_low_frac")
            street_predir = _strfy("street_predir")
            street_name = _strfy("street_name")
            street_suffix = _strfy("street_suffix")
            full_address = (
                f"{address_low}{address_low_suffix}{address_high} {address_low_frac}"
                f" {street_predir} {street_name} {street_suffix}"
            )
            return " ".join(full_address.split())

        discrepencies = []
        if not df.empty:
            df["composed_address"] = df.apply(_compose_address, axis=1)
            if not df[df["composed_address"] != df["street_address"]].empty:
                # If it finds a non-matching address:
                data = df[df["composed_address"] != df["street_address"]][
                    [
                        "composed_address",
                        "street_address",
                        "opa_account_num",
                        "recording_date",
                    ]
                ].to_dict("records")
                discrepencies.append(
                    {
                        "data": data,
                        "description": "There was an address associated with a DEED "
                        "that didn't seem to match the property.",
                    }
                )
            df_match = df[df["composed_address"] == df["street_address"]]
        if df.empty or df_match.empty:
            discrepencies.append(
                {
                    "data": {
                        "opa_account_num": opa_account_number,
                        "recording_date": recording_date,
                    },
                    "description": "There is no DEED information available for this property.",
                }
            )
            return {
                "owner": None,
                "metadata": None,
                "discrepencies": discrepencies,
            }
        else:
            df_before = df_match[df_match["recording_date"] < recording_date]
            if df_before.empty:
                discrepencies.append(
                    {
                        "description": (
                            "The DEED for this property during this time was transferred"
                            " earlier than this dataset has acccess (before 1999)."
                        )
                    }
                )
                df_out = df_match.copy().iloc[0]
                df_out["owner"] = df_out["grantors"]
            else:
                df_out = df_before.copy().iloc[-1]
                df_out["owner"] = df_out["grantees"]
        output_dict = df_out[
            [
                "owner",
                "grantors",
                "grantees",
                "recording_date",
                "opa_account_num",
                "composed_address",
                "street_address",
            ]
        ].to_dict()
        return {
            "owner": output_dict["owner"],
            "metadata": output_dict,
            "discrepencies": discrepencies,
        }


class CaseInvestigations(PhillyCartoTable):
    def __init__(self, title="Case Investigations"):
        super().__init__(
            cartodb_table_name="case_investigations",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-inspections",
            schema_application_id="5543ca785c4ae4cd66d3ff80",
            schema_representation_id="5e986970b2c39b001522fb9d",
        )
        self.default_columns = [
            "investigationcompleted",
            "casenumber",
            "investigationprocessid",
            "casetype",
            "caseresponsibility",
            "casepriority",
            "investigationtype",
            "investigationstatus",
        ]

        self.dt_column = "investigationcompleted"


class PropertiesPde(PhillyCartoTable):
    def __init__(self, title="Properties (Cleaned)"):
        """
        This query always returns latitude and longitude in addition to any other
        columns requested.
        """
        super().__init__(
            cartodb_table_name="opa_properties_public_pde",
            title=title,
            sql_alias="opa",  # other pieces of code rely on this to be 'opa'
        )
        self.default_columns = [
            "address_std",
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
            "political_district",
            "police_district",
            "elementary_school",
            "middle_school",
            "high_school",
            "parcel_number",
        ]

    def _get_sql_for_query_by_opa_account_numbers(
        self, opa_account_numbers, col_str, joined_col_str
    ):
        # no joining necessary
        return f"""
        SELECT  {col_str}, ST_Y(the_geom) AS lat, ST_X(the_geom) AS lng
        FROM {self.cartodb_table_name} {self.sql_alias}
        WHERE parcel_number in ({opa_account_numbers})
        """
