from .abstract import PhiladelphiaDataTable


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


class Properties(PhiladelphiaDataTable):
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


class Licenses(PhiladelphiaDataTable):
    def __init__(self, title="Licenses"):
        super().__init__(
            cartodb_table_name="business_licenses",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-business-licenses",
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


class Condominiums(PhiladelphiaDataTable):
    def __init__(self, title="Condominiums"):
        super().__init__(
            cartodb_table_name="condominium", title=title,
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


class Complaints(PhiladelphiaDataTable):
    def __init__(self, title="Complaints"):
        super().__init__(
            cartodb_table_name="complaints",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-service-requests",
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
        self.dt_col = "complaintdate"


class Violations(PhiladelphiaDataTable):
    def __init__(self, title="Violations"):
        super().__init__(
            cartodb_table_name="violations",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-violations",
        )
        self.default_columns = [
            "violationdate",
            "caseprioritydesc",
            "violationcode",
            "violationcodetitle",
        ]
        self.dt_col = "violationdate"


class Permits(PhiladelphiaDataTable):
    def __init__(self, title="Permits"):
        super().__init__(
            cartodb_table_name="permits",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-building-permits",
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
        self.dt_col = "permitissuedate"


class Appeals(PhiladelphiaDataTable):
    def __init__(self, title="Appeals"):
        super().__init__(
            cartodb_table_name="appeals",
            title=title,
            open_data_philly_table_url_name="license-and-inspections-appeals",
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

        self.dt_col = "createddate"


class RealEstateTaxDelinquencies(PhiladelphiaDataTable):
    def __init__(self, title="Real Estate Tax Delinquencies"):
        super().__init__(
            cartodb_table_name="real_estate_tax_delinquencies",
            title=title,
            open_data_philly_table_url_name="property-tax-delinquencies",
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

        self.dt_col = "most_recent_year_owed"

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


class RealEstateTransfers(PhiladelphiaDataTable):
    def __init__(self, title="Real Estate Transfers"):
        super().__init__(
            cartodb_table_name="rtt_summary",
            title=title,
            open_data_philly_table_url_name="real-estate-transfers",
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
        ]

        self.dt_col = "receiptdate"


class CaseInvestigations(PhiladelphiaDataTable):
    def __init__(self, title="Case Investigations"):
        super().__init__(
            cartodb_table_name="case_investigations",
            title=title,
            open_data_philly_table_url_name="licenses-and-inspections-inspections",
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

        self.dt_col = "investigationcompleted"


class PropertiesPde(PhiladelphiaDataTable):
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


