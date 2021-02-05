# phillydb

## Data

<p>This library is a python client that provides up-to-date open data from the City of Philadelphia. The data comes from the following sources:</p>
<ul>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/opa-property-assessments'>opa-property-assessments</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#opa_properties_public'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/licenses-and-inspections-violations'>licenses-and-inspections-violations</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#violations'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/licenses-and-inspections-business-licenses'>licenses-and-inspections-business-licenses</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#business_licenses'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/licenses-and-inspections-building-permits'>licenses-and-inspections-building-permits</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#permits'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/license-and-inspections-appeals'>license-and-inspections-appeals</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#appeals'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/licenses-and-inspections-service-requests'>licenses-and-inspections-service-requests</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#complaints'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/real-estate-transfers'>real-estate-transfers</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#RTT_SUMMARY'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/property-tax-delinquencies'>property-tax-delinquencies</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#real_estate_tax_delinquencies'>CartoDB</a>)</li>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/licenses-and-inspections-inspections'>licenses-and-inspections-inspections</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#case_investigations'>CartoDB</a>)</li>
<li>Condominiums (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#condominium'>CartoDB</a>)</li>
</ul>

### Potential Future Sources
<ul>
<li><a target='_blank' href='https://www.opendataphilly.org/dataset/311-service-and-information-requests'>311 Requests</a> (<a target='_blank' href='https://cityofphiladelphia.github.io/carto-api-explorer/#public_cases_fc'>CartoDB</a>)</li>
</ul>

### Is public housing data included in this library?

This tool attempts to remove any property that is owned by the city.
However, city property is poorly labeled in the data, which makes it hard to find and remove.

Check out the [exclusion list](https://github.com/ssuffian/phillydb/blob/main/phillydb/exclusion_filters.py) for the list of terms that are connected to public housing and therefore automatically excluded.

## Tests
There are two "types" of tests. One actually hits the URLs, the other mocks the request. By default, the mocked request is used. To hit the URLs, use the `--make-request` option.
```
./test.sh
./test.sh --make-request
```
