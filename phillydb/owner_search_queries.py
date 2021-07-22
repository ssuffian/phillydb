from fuzzywuzzy import fuzz
import numpy as np
import pandas as pd
import requests


def score_owners(x, search_name):
    # TODO - more complex rules about scoring and providing info on the relevance of owners
    return np.mean([fuzz.partial_ratio(x["owner_name"], search_name)])


class OwnerQuery:
    """Step 1: Takes owner names and generates a parcel num sql query"""

    def __init__(self, owner_names, include_owners_with_same_mailing_address=True):
        # if passed a single owner name, put it in a list
        if type(owner_names) == str:
            owner_names = [owner_names]
        self.owner_names = owner_names

        self.parcel_num_sql_list = []
        self.owner_df = None
        self.include_owners_with_same_mailing_address = (
            include_owners_with_same_mailing_address
        )
        self._set_owner_df_and_parcel_num_sql()
        self.owners_list = self.owner_df["owner_name"].unique()

        # Step 1.5: At this point, users can manually exclude owner names
        # using the owners_list. (TODO)
        # They would modify the parcel_num_sql_lists below
        all_parcel_num_sql = "\nUNION ALL\n".join(self.parcel_num_sql_list)
        self.parcel_num_sql = f"SELECT distinct(parcel_number) from ({all_parcel_num_sql}) as unique_parcels"

    def _set_owner_df_and_parcel_num_sql(self):
        def _sql_string_replace(x):
            # FOR DIRECT OWNER SEARCHING
            # SOLVING THE ISSUE WHERE OWNERS HAVE & IN THE NAME
            return x.replace("&", "'|| chr(38) ||'")

        owner_dfs = []
        for search_name in self.owner_names:
            owner_name = _sql_string_replace(search_name)
            single_owner_df, single_parcel_num_sql = get_complete_owner_list(
                owner_name,
                include_opa_properties_public=True,
                include_rtt_summary=True,
                include_business_licenses=True,
                opa_properties_public_matches_on_mailing_address=self.include_owners_with_same_mailing_address,
            )
            if not single_owner_df.empty:
                single_owner_df["score"] = single_owner_df.apply(
                    lambda x: score_owners(x, search_name), axis=1
                )
                owner_dfs.append(single_owner_df)
                self.parcel_num_sql_list.append(single_parcel_num_sql)
        self.owner_df = (
            pd.concat(owner_dfs)
            .sort_values("score", ascending=False)
            .drop_duplicates("owner_name")
            if owner_dfs
            else pd.DataFrame(columns=["owner_name"])
        )


class OwnerQueryResult:
    """Step 2: Get a timeline of when the owner owned properties"""

    def __init__(self, parcel_num_sql, owners_list):
        self.parcel_num_sql = parcel_num_sql
        self.owners_list = owners_list

        deeds_df = get_deeds_list(parcel_num_sql)

        def _deed_timeline(parcel_df):
            parcel_df["start_dt"] = parcel_df["deed_date"]
            parcel_df["end_dt"] = parcel_df["start_dt"].shift(-1)
            first_owner = parcel_df.iloc[0].copy(deep=True).to_dict()
            first_owner["likely_owner"] = (
                first_owner["grantors"] if first_owner["grantors"] else "?"
            )
            # if year built is 0000, assume it was constructed at the first deed
            first_owner["start_dt"] = (
                first_owner["year_built"]
                if first_owner["year_built"] != "0000"
                else first_owner["deed_date"]
            )
            first_owner["end_dt"] = first_owner["deed_date"]

            combined_df = pd.concat(
                [
                    parcel_df,
                    pd.DataFrame([first_owner])[
                        ["parcel_number", "likely_owner", "start_dt", "end_dt"]
                    ],
                ],
                sort=False,
            ).sort_values(["parcel_number", "end_dt"])
            combined_df["street_address"] = combined_df["street_address"].bfill()
            combined_df["location"] = combined_df["location"].bfill()
            combined_df["lat"] = combined_df["lat"].bfill()
            combined_df["lng"] = combined_df["lng"].bfill()
            combined_df["unit"] = combined_df["unit"].bfill()
            return combined_df

        timeline_df = deeds_df.groupby("parcel_number").apply(_deed_timeline)[
            [
                "parcel_number",
                "grantees",
                "owner_1",
                "owner_2",
                "likely_owner",
                "start_dt",
                "end_dt",
                "street_address",
                "location",
                "lat",
                "lng",
                "unit",
                "unit_num",
            ]
        ]

        def _check_owner_names_are_equivalent(owner_1, owner_2):
            # owner in opa_properties_public get cut off at 25 chars
            return owner_1 == owner_2 or (
                len(owner_1) == 25 and owner_2.startswith(owner_1)
            )

        def _check_owner_in_list(x, owners_list):
            return any(
                [
                    _check_owner_names_are_equivalent(poss_owner, part_owner)
                    for poss_owner in owners_list
                    for part_owner in x.split(";")
                ]
            )

        owners_timeline_df = timeline_df[
            timeline_df["likely_owner"].apply(
                lambda x: _check_owner_in_list(x, self.owners_list)
            )
        ].sort_index()

        self.timeline_df = timeline_df
        self.owners_timeline_df = owners_timeline_df
        self.deeds_df = deeds_df

    def get_filtered_df(self, df_table, dt_col):
        if df_table.empty:
            return pd.DataFrame()
        else:
            results_df = (
                self.owners_timeline_df.set_index("parcel_number")[
                    ["likely_owner", "start_dt", "end_dt"]
                ]
                .merge(df_table, how="left", on="parcel_number")
                .set_index("parcel_number")
                .dropna(subset=[dt_col])
            )

            results_df[dt_col] = pd.to_datetime(
                results_df[dt_col], utc=True
            ) + pd.Timedelta(
                days=1
            )  # 888035586 off by a day licenses
            results_df["start_dt"] = pd.to_datetime(results_df["start_dt"], utc=True)
            results_df["end_dt"] = pd.to_datetime(results_df["end_dt"], utc=True)
            filtered_results_df = results_df[
                (results_df[dt_col] > results_df["start_dt"])
                & (
                    (
                        pd.isnull(results_df["end_dt"])
                        | (results_df[dt_col] < results_df["end_dt"])
                    )
                )
            ]
            return filtered_results_df


def get_complete_owner_list(
    owner_name,
    include_opa_properties_public=True,
    include_rtt_summary=True,
    include_business_licenses=True,
    opa_properties_public_matches_on_mailing_address=False,
):
    owners_list = []
    parcel_num_subquery_list = []
    # opa_owners
    if include_opa_properties_public:
        opa_properties_public_query = f"""
            SELECT owner_1, owner_2, parcel_number
            FROM opa_properties_public
            WHERE owner_1 like '{owner_name}' or owner_2 like '{owner_name}'
        """
        opa_owners_result = requests.get(
            f"""
            https://phl.carto.com/api/v2/sql?q={opa_properties_public_query}
            """
        ).json()["rows"]
        opa_owner_1_list = list(
            set([r["owner_1"] for r in opa_owners_result if r["owner_1"]])
        )
        opa_owner_2_list = list(
            set([r["owner_2"] for r in opa_owners_result if r["owner_2"]])
        )
        owners_list.extend(
            [
                {"owner_name": y.strip(), "source": "opa_properties_public.owner_1"}
                for x in opa_owner_1_list
                for y in x.split(";")
            ]
            + [
                {"owner_name": y.strip(), "source": "opa_properties_public.owner_2"}
                for x in opa_owner_2_list
                for y in x.split(";")
            ]
        )
        parcel_num_subquery_list.append(
            f"SELECT distinct(inner_search_opa.parcel_number) from ({opa_properties_public_query}) inner_search_opa"
        )
    if opa_properties_public_matches_on_mailing_address:
        opa_properties_public_query = f"""
            SELECT owner_1, owner_2, parcel_number
            FROM opa_properties_public
            WHERE mailing_street in (
                SELECT mailing_street from opa_properties_public
                WHERE owner_1 like '{owner_name}' or owner_2 like '{owner_name}'
            )
            """
        opa_owners_result = requests.get(
            f"""
            https://phl.carto.com/api/v2/sql?q={opa_properties_public_query}
            """
        ).json()["rows"]
        opa_owner_1_list = list(
            set([r["owner_1"] for r in opa_owners_result if r["owner_1"]])
        )
        opa_owner_2_list = list(
            set([r["owner_2"] for r in opa_owners_result if r["owner_2"]])
        )
        owners_list.extend(
            [
                {"owner_name": y.strip(), "source": "opa_properties_public.owner_1"}
                for x in opa_owner_1_list
                for y in x.split(";")
            ]
            + [
                {"owner_name": y.strip(), "source": "opa_properties_public.owner_2"}
                for x in opa_owner_2_list
                for y in x.split(";")
            ]
        )
        parcel_num_subquery_list.append(
            f"SELECT distinct(inner_search_opa.parcel_number) from ({opa_properties_public_query}) inner_search_opa"
        )

    # rtt summary
    if include_rtt_summary:
        rtt_summary_query = f"""
            SELECT grantees, opa_account_num
            FROM rtt_summary
            WHERE grantees like '{owner_name}'
        """
        rtt_summary_result = requests.get(
            f"""
            https://phl.carto.com/api/v2/sql?q={rtt_summary_query}
            """
        ).json()["rows"]
        grantees_list = list(set([r["grantees"] for r in rtt_summary_result]))
        grantors_list = list(set([r["grantees"] for r in rtt_summary_result]))
        owners_list.extend(
            [
                {"owner_name": y.strip(), "source": "rtt_summary.grantees"}
                for x in grantees_list
                for y in x.split(";")
            ]
            + [
                {"owner_name": y.strip(), "source": "rtt_summary.grantors"}
                for x in grantors_list
                for y in x.split(";")
            ]
        )
        parcel_num_subquery_list.append(
            f"SELECT distinct(inner_search_rtt.opa_account_num) from ({rtt_summary_query}) inner_search_rtt"
        )

    # licenses
    if include_business_licenses:
        business_licenses_query = f"""
            SELECT opa_owner, business_name, legalname, licensestatus, opa_account_num
            FROM business_licenses
            WHERE (
                legalname like '{owner_name}' 
                OR business_name like '{owner_name}' 
                /*OR opa_owner like '{owner_name}' opa_owner is the current owner, not the one at the time of the license*/
            ) and licensetype = 'Rental'
        """
        li_owner_result = requests.get(
            f"""
            https://phl.carto.com/api/v2/sql?q={business_licenses_query}
            """
        ).json()["rows"]

        # include opa_owner field if license status is active, which implies that the license occurred
        # while the current owner owned that building
        li_opa_owner_list = list(
            set(
                [
                    r["opa_owner"]
                    for r in li_owner_result
                    if r["opa_owner"] and r["licensestatus"] == "Active"
                ]
            )
        )
        li_legal_name_list = list(
            set([r["legalname"] for r in li_owner_result if r["legalname"]])
        )
        li_business_name_list = list(
            set([r["business_name"] for r in li_owner_result if r["business_name"]])
        )
        owners_list.extend(
            [
                {"owner_name": y.strip(), "source": "licenses.opa_owner"}
                for x in li_opa_owner_list
                for y in x.split(";")
            ]
            + [
                {"owner_name": y.strip(), "source": "licenses.legalname"}
                for x in li_legal_name_list
                for y in x.split(";")
            ]
            + [
                {"owner_name": y.strip(), "source": "licenses.business_name"}
                for x in li_business_name_list
                for y in x.split(";")
            ]
        )
        parcel_num_subquery_list.append(
            f"SELECT distinct(inner_search_li.opa_account_num) from ({business_licenses_query}) inner_search_li"
        )
    if owners_list:
        owner_df = (
            pd.DataFrame(owners_list)
            .drop_duplicates("owner_name")
            .sort_values("owner_name")
        )
        parcel_num_subquery = "\nUNION ALL\n".join(parcel_num_subquery_list)
        parcel_num_sql = f"""
            SELECT distinct(parcel_number)
            FROM opa_properties_public
            where parcel_number in  ({parcel_num_subquery})
        """
        return owner_df, parcel_num_sql
    else:
        return pd.DataFrame(), "SELECT distinct(parcel_number) WHERE 1=0"


def get_deeds_list(parcel_numbers_str):
    """
    Single SQL query attempt

    select estimated_owner, date, parcel_number from

    property assessment table joined to rtt table fields that are used in the where clause

    group by every property

    and get the
    case 1:
        latest grantee before XXX-date
    case2:
        earliest grantor
    case 3:
        owner_1;owner_2
    """
    where = """
    (

        (
            /* The adddress_low between rtt and opa match (usually the case on exact address match) */
            cast(rtt.ADDRESS_LOW as int) >= opa.address_low AND cast(rtt.ADDRESS_LOW as int) <= opa.address_low
        ) OR (
            /* If the opa address is exact but the rtt address is a span like 3608-14 X St */
            cast(rtt.ADDRESS_LOW as int)>=  opa.address_low - (MOD(opa.address_low, 100))
            AND cast(rtt.ADDRESS_HIGH as int) >= MOD(opa.address_low, 100) 
            AND cast(rtt.ADDRESS_LOW as int) <= ( 
                CASE 
                    WHEN opa.address_high is not null
                    THEN opa.address_high + opa.address_low - (MOD(opa.address_low, 100))
                ELSE opa.address_low
                END   
            )
        )
    )

    /* street names match, opa.street_designation sometimes has trailing spaces so use RTRIM */
    /* 182108500 is one of 126000 that has 0Xth in opa and Xth in rtt so need to LTRIM */
    AND (rtt.STREET_NAME = LTRIM(opa.street_name, '0') )
    AND  MOD(cast(rtt.ADDRESS_LOW as int),2) = MOD(opa.address_low,2) /* Check on correct side of street */
    /* 52338901 example of street suffix being wrong so also match on full street address */
    AND (
            rtt.STREET_SUFFIX = RTRIM(opa.street_designation) 
            OR rtt.STREET_ADDRESS = concat(opa.location) 
    )
    AND (
        CASE
            WHEN opa.street_direction is not null THEN (
                rtt.STREET_PREDIR = opa.street_direction or rtt.STREET_POSTDIR = opa.street_direction
            ) 
            ELSE True
        END
    )
    AND (
        CASE
            WHEN opa.suffix is not null and opa.suffix = '2' THEN (
                rtt.ADDRESS_LOW_SUFFIX = '2' or rtt.ADDRESS_LOW_FRAC = '1/2'
            ) 
            /* had to add 'like' becuase 4604R WHITAKER AVE had RR as the rtt.address_low_suffix */
            WHEN opa.suffix is not null THEN  rtt.ADDRESS_LOW_SUFFIX like concat(opa.suffix, '%')
            ELSE rtt.ADDRESS_LOW_SUFFIX is null and rtt.ADDRESS_LOW_FRAC is null
        END
    )
    AND CASE WHEN opa.unit is not null THEN rtt.UNIT_NUM = opa.unit ELSE True END
    AND
    (
        DOCUMENT_TYPE ='DEED' or 
        DOCUMENT_TYPE='DEED SHERIFF' or 
        DOCUMENT_TYPE='DEED OF CONDEMNATION' or 
        DOCUMENT_TYPE='DEED LAND BANK'
    )
    """
    query = f"""
    SELECT opa.owner_1,opa.owner_2, opa.parcel_number, opa.year_built, 
    ST_Y(opa.the_geom) AS lat, ST_X(opa.the_geom) AS lng,
    rtt.opa_account_num, rtt.unit_num,
    rtt.grantors, rtt.grantees, 
    rtt.street_address, rtt.recording_date,
    opa.location, opa.unit,
    CASE
        WHEN rtt.recording_date is null THEN opa.recording_date
        ELSE rtt.recording_date
    END as deed_date,
    CASE
        WHEN rtt.recording_date is null THEN RTRIM(concat(opa.owner_1,';',opa.owner_2), ';')
        ELSE rtt.grantees
    END as likely_owner
    FROM (
        SELECT * FROM rtt_summary
    ) rtt
    RIGHT JOIN (
        SELECT
        the_geom,
        parcel_number,
        year_built,
        owner_1, owner_2,
        location,
        category_code_description, zoning,
        cast(house_number as int) as address_low,
        cast(house_extension as int) as address_high,
        street_direction,
        street_name,
        street_designation,
        unit, 
        suffix, 
        recording_date
        FROM opa_properties_public
    ) opa ON
    {where}
    WHERE opa.parcel_number in ({parcel_numbers_str})
    GROUP BY opa.owner_1,opa.owner_2, opa.parcel_number, opa.year_built, 
    lat, lng,
    rtt.opa_account_num,
        rtt.grantors, rtt.grantees, rtt.street_address, opa.location, opa.unit, rtt.unit_num,
        rtt.recording_date, opa.recording_date
    ORDER BY opa.parcel_number, rtt.recording_date
    """
    params = {"q": query}
    opa_properties_deeds = requests.get(
        f"""https://phl.carto.com/api/v2/sql""", params=params
    ).json()
    try:
        return pd.DataFrame(opa_properties_deeds["rows"])
    except:
        raise ValueError(f"{opa_properties_deeds}\n\n{parcel_numbers_str}\n{query}")
