# phillydb

## Tests
There are two "types" of tests. One actually hits the URLs, the other mocks the request. By default, the mocked request is used. To hit the URLs, use the `--make-request` option.
```
./test.sh
./test.sh --make-request
```
