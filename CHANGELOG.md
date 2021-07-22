Development
-----------

* Placeholder

0.2.2
-----

* Add lat and lng as outputs of owner search query.
* Move li.phila.gov link from parcel number to address.

0.2.1
-----

* Added fuzzywuzzy and us-scourgify dependencies.
* Added owner-based search.

0.2.0
-----

* Accidental version bump.

0.1.0
-----

* Remove default removal of city-owned proeprties.
* Refactor tests.
* Add address formatting using passyunk.

0.0.7
-----

* Add link to table schema sites and a function to get the schema for each table using `get_schema()`.
* Add list() to each table.
* Update testing framework to be more general for future tests.
* Add RealEstateTransfers().infer_property_ownership() which allows for historical ownership querying.

0.0.6
-----

* Add property-specific links to li, cyclomedia, and atlas.
* Add RealEstateTaxRevenue endpoint.

0.0.5
-----

* Add wheel builder to bump_version.sh

0.0.4
-----

* Update bump_version.sh script with `python setup.py sdist`.

0.0.3
-----

* Add another query that can be used for autocomplete `query_by_single_str_column`.
* Remove constraint for owner search on rental licenses that are active.
* Rename PhiladelphiaDataTable to PhiladelphiaCartoDataTable

0.0.2
-----

* Add custom exceptions.

0.0.1
-----

* Iniital release.
