# pg_back dumps databases from PostgreSQL

## Description

pg_back is a dump tool for PostgreSQL. The goal is to dump all or some
databases with globals at once in the format you want, because a simple call to
pg_dumpall only dumps databases in the plain SQL format.

Behind the scene, pg_back uses `pg_dumpall` to dump roles and tablespaces
definitions, `pg_dump` to dump all or each selected database to a separate file
in the custom format. It also extract database level ACL and configuration that
is not dumped by pg_dump older than 11. Finally, it dumps all configuration
options of the PostgreSQL instance.

## Features

* Dump all or a list of databases
* Dump all but a list of excluded databases
* Include database templates
* Choose the format of the dump for each database
* Limit dumped schemas and tables
* Dump databases concurrently
* Compute a SHA checksum of each dump
* Pre-backup and post-backup hooks
* Purge based on age and number of dumps to keep
* Dump from a hot standby by pausing replication replay

## Install

A compiled binary is available from the [Github repository](https://github.com/orgrim/pg_back/releases).

The binary only needs `pg_dumpall` and `pg_dump`.

## Install from source

```
go get -u github.com/orgrim/pg_back
```

Use `make` to build and install from source (you need go 1.16 or above).

As an alternative, the following *docker* command downloads, compiles and puts `pg_back`
in the current directory:

```
docker run --rm -v "$PWD":/go/bin golang:1.16 go get github.com/orgrim/pg_back
```

## Minimum versions

The minimum version of `pg_dump` et `pg_dumpall` required to dump is 8.4. The
oldest tested server version of PostgreSQL is 8.2.

## Usage

Use the `--help` or `-?` to print the list of available options. To dump all
databases, you only need to give the proper connection options to the PostgreSQL
instance and the path to a writable directory to store the dump files.

If default and command line options are not enough, a configuration file
may be provided with `-c <configfilename>`.
(Note: see below to convert configuration files from version 1.)

If the default output directory `/var/backups/postgresql` does not exist or has
improper ownership for your user, use `-b` to give the path where to store the
files. The path may contain the `{dbname}` keyword, that would be replaced by
the name of the database being dumped, this permits to dump.

To connect to PostgreSQL, use the `-h`, `-p`, `-U` and `-d` options. If you
need less known connection options such as `sslcert` and `sslkey`, you can give
a `keyword=value` libpq connection string like `pg_dump` and `pg_dumpall`
accept with their `-d` option. When using connection strings, backslashes must
be escaped (doubled), as well as literal single quotes (used as string
delimiters).

The other command line options let you tweak what is dumped, purged, and how
it is done. These options can be put in a configuration file. The command line
options override configuration options.

Per-database configuration can only be done with a configuration file. The
configuration file uses the `ini` format, global options are in a unspecified
section at the top of the file, and database specific options are in a section
named after the database. Per database options override global options of the
configuration file.

In database sections of the configuration file, a list of schemas or tables can
be excluded from or selected in the dump. When using these options, the rules
of the `-t`, `-T`, `-n` and `-N` of `pg_dump` and pattern rules apply. See the
[documentation of `pg_dump`][pg_dump].

When no databases names are given on the command line, all databases except
templates are dumped. To include templates, use `--with-templates` (`-T`), if
templates are includes from the configuration file, `--without-templates` force
exclude them.

Databases can be excluded with `--exclude-dbs` (`-D`), which is a comma separated list
of database names. If a database is listed on the command line and part of
exclusion list, exclusion wins.

Multiple databases can be dumped at the same time, by using a number of
concurrent `pg_dump` jobs greater than 1 with `--jobs` (`-j`) option. It is different
than `--parallel-backup-jobs` (`-J`) that controls the number of sessions used by
`pg_dump` with the directory format.

A checksum of all output files is computed in a separate file when
`--checksum-algo` (`-S`) is different than `none`. The possible algorithms are:
`sha1`, `sha224`, `sha256`, `sha384` and `sha512`. The checksum file is in the
format required by _shaXsum_ (`sha1sum`, `sha256sum`, etc.) tools for checking
with their `-c` option.

Older dumps can be removed based on their age with `--purge-older-than` (`-P`)
in days, if no unit is given. Allowed units are the ones understood by the
`time.ParseDuration` Go function: "s" (seconds), "m" (minutes), "h" (hours) and
so on.

A number of dump files to keep when purging can also be specified with
`--purge-min-keep` (`-K`) with the special value `all` to keep everything, thus
avoiding file removal completly. When both `--purge-older-than` and
`--purge-min-keep` are used, the minimum number of dumps to keep is enforced
before old dumps are removed. This avoids removing all dumps when the time
interval is too small.

A command can be run before taking dumps with `--pre-backup-hook`, and after
with `--post-backup-hook`. The commands are executed directly, not by a shell,
respecting single and double quoted values. Even if some operation fails, the
post backup hook is executed when present.

## Managing the configuration file

The previous v1 configuration files are not compatible with pg_back v2.

Give the path of the v1 configuration file to the `--convert-legacy-config`
command line option, and pg_back will try its best to convert it to the v2
format. Redirect the output to the new configuration file:

```
pg_back --convert-legacy-config  pg_back1.conf > pg_back2.conf
```

The default configuration file can be printed with the `--print-default-config`
command line option.

On some environments (especially Debian), you may have to add `host = /var/run/postgresql`
to override the default `/tmp` host.

## Testing

Use the Makefile or regular `go test`.

To run SQL tests requiring a PostgreSQL instance:

1. run `initdb` in some directory
2. start `postgres`
3. load `testdata/fixture.sql` with `psql`
4. use `go test` or `make test` with the `PGBK_TEST_CONNINFO` environment
   variable set to a libpq connection string pointing to the instance. For
   example :

```
PGBK_TEST_CONNINFO="host=/tmp port=14651" make test
```

## Contributing

Please use the issues and pull requests features from Github.

## License

PostgreSQL - See [LICENSE][license] file

[license]: https://github.com/orgrim/pg_back/blob/master/LICENSE
[pg_dump]: https://www.postgresql.org/docs/current/app-pgdump.html
