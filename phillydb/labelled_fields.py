CITY_OWNED_PROPERTY_FIELDS = {
    "owner_1": [
        "CITY OF PHILA",
        "PHILA HOUSING AUTHORITY",
        "CITY OF PHILADELPHIA",
        "PHILADELPHIA HOUSING",
        "REDEVELOPMENT AUTHORITY",
        "REDEVELOPMENT AUTHORITY O",
        "SCHOOL DISTRICT OF PHILA",
        "PHILADELPHIA HOUSING AUTH",
        "PHILA HOUSING AUTHORITY",
        "PHILADELPHIA LAND BANK",
        "PHILADELPHIA HOUSING DEVE",
        "CITY OC PHILA",
        "CITY HALL ANNEX",
    ],
    "mailing_street": [
        "12 S 23RD ST",
        "1401 JOHN F KENNEDY BLVD",
        "790 CITY HALL",
        "CITY HALL ANNEX",
        "CITY HALL RM 784",
        "CITY HALL",
        "723 CITY HALL ANNEX",
        "3100 PENROSE FERRY RD",
    ],
}


def label_city_owned_properties(df, remove=False, city_owned_label="is_city_owned"):
    """Checks to see if the property is owned by the city.

    This requires the input dataframe to have columns specified in
    CITY_OWNED_PROPERTY_FIELDS

    Parameters
    ----------
    df: pd.DataFrame
        A dataframe of properties that contains data allowing it to be labelled as
        city-owned or not.
    remove: bool
        Whether to remove properties identified as city-owned from the returned dataframe.
    city_owned_label: str
        The column name to include the boolean of whether each property is city-owned.

    Returns
    -------
    pd.DataFrame
        Returns a dataframe with a `is_city_owned

    """
    city_owned_required_cols = CITY_OWNED_PROPERTY_FIELDS.keys()
    for column, values in CITY_OWNED_PROPERTY_FIELDS.items():
        if column not in df.columns:
            raise ValueError(
                f"{column} not in results. You must "
                f"request {city_owned_required_cols} columns "
                "in order to check for city-owned properties."
            )
        city_owned_query = f"{column}.str.strip() not in {tuple(values)}"
        if remove:
            df = df.query(city_owned_query)
        else:
            df[city_owned_label] = False
            df.loc[df.query(city_owned_query)] = True
    return df
