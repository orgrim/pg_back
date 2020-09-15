# pg_goback dumps databases from PostgreSQL

## Install

```
go get -u github.com/orgrim/pg_goback
```

Use `make` to build and install from source as an alternative.

## Testing

Use the Makefile or regular `go test`.

To run SQL tests requiring a PostgreSQL instance:

1. run `initdb` in some directory
2. start `postgres`
3. load `testdata/fixture.sql` with `psql`
4. use `go test` or `make test` with the `PGBK_TEST_CONNINFO` environment variable set to a libpq connection string pointing to the instance. For example :

```
PGBK_TEST_CONNINFO="host=/tmp port=14651" make test
```

## License

PostgreSQL - See [LICENSE][license] file

[license]: https://github.com/orgrim/pg_goback/LICENSE
