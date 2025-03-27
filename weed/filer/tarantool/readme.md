## Tarantool

database: https://www.tarantool.io/

go driver: https://github.com/tarantool/go-tarantool/

To set up local env:
`make -C docker test_tarantool`

Run tests:
`RUN_TARANTOOL_TESTS=1 go test -tags=tarantool ./weed/filer/tarantool`