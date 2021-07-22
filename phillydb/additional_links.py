def get_street_view_link(location):
    return f"https://cyclomedia.phila.gov/#/?address={location}"


def get_property_phila_gov_link(opa_account_number):
    return f"https://property-beta.phila.gov/#/?owner={opa_account_number}"


def get_atlas_link(opa_account_number):
    return f"https://atlas.phila.gov/{opa_account_number}"


def get_license_inspections_link(location):
    return f"https://li.phila.gov/property-history/search?address={location}"
