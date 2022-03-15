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
* Encrypt and decrypt dumps and other files
* Upload dumps to S3, GCS, Azure or a remote host with SFTP

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

### Basic usage

Use the `--help` or `-?` to print the list of available options. To dump all
databases, you only need to give the proper connection options to the PostgreSQL
instance and the path to a writable directory to store the dump files.

If default and command line options are not enough, a configuration file
may be provided with `-c <configfilename>` (see [pg_back.conf](pg_back.conf)).
(Note: see below to convert configuration files from version 1.)

If the default output directory `/var/backups/postgresql` does not exist or has
improper ownership for your user, use `-b` to give the path where to store the
files. The path may contain the `{dbname}` keyword, that would be replaced by
the name of the database being dumped, this permits to dump each database in
its own directory.

To connect to PostgreSQL, use the `-h`, `-p`, `-U` and `-d` options. If you
need less known connection options such as `sslcert` and `sslkey`, you can give
a `keyword=value` libpq connection string like `pg_dump` and `pg_dumpall`
accept with their `-d` option. When using connection strings, backslashes must
be escaped (doubled), as well as literal single quotes (used as string
delimiters).

The other command line options let you tweak what is dumped, purged, and how
it is done. These options can be put in a configuration file. The command line
options override configuration options.

### Per-database configuration

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

### Checksums

A checksum of all output files is computed in a separate file when
`--checksum-algo` (`-S`) is different than `none`. The possible algorithms are:
`sha1`, `sha224`, `sha256`, `sha384` and `sha512`. The checksum file is in the
format required by _shaXsum_ (`sha1sum`, `sha256sum`, etc.) tools for checking
with their `-c` option.

### Purge

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

### Hooks

A command can be run before taking dumps with `--pre-backup-hook`, and after
with `--post-backup-hook`. The commands are executed directly, not by a shell,
respecting single and double quoted values. Even if some operation fails, the
post backup hook is executed when present.

### Encryption

All the files procuded by a run of pg_back can be encrypted using age
(<https://age-encryption.org/> an easy to use tool that does authenticated
encryption of files). To keep things simple, encryption is done using a
passphrase. To encrypt files, use the `--encrypt` option along with the
`--cipher-pass` option or `PGBK_CIPHER_PASS` environment variable to specify the
passphrase. When `encrypt` is set to true in the configuration file, the
`--no-encrypt` option allows to disable encryption on the command line. By
default, unencrypted source files are removed when they are successfully
encrypted. Use the `--encrypt-keep-src` option to keep them or
`--no-encrypt-keep-src` to force remove them and override the configuration
file. If required, checksum of encrypted files are computed.

Encrypted files can be decrypted with the correct passphrase and the
`--decrypt` option. When `--decrypt` is present on the command line, dumps are
not performed, instead files are decrypted. Files can also be decrypted with
the `age` tool, independently. Decryption of multiple files can be parallelized
with the `-j` option. Arguments on the commandline (database names when
dumping) are used as shell globs to choose which files to decrypt.

**Please note** that files are written on disk unencrypted in the backup directory,
before encryption and deleted after the encryption operation is complete. This
means that the host running `pg_back` must secure enough to ensure privacy of the
backup directory and connections to PostgreSQL.

### Upload to remote locations

All files produced by a run can be uploaded to a remote location by setting the
`--upload` option to a value different than `none`. The possible values are
`s3`, `sftp`, `gcs`, `azure` or `none`.

When set to `s3`, files are uploaded to AWS S3. The `--s3-*` family of options
can be used to tweak the access to the bucket. The `--s3-profile` option only
reads credentials and basic configuration, s3 specific options are not used.

When set to `sftp`, files are uploaded to a remote host using SFTP. The
`--sftp-*` family of options can be used to setup the access to the host. The
`PGBK_SSH_PASS` sets the password or decrypts the private key (identity file),
it is used only when `--sftp-password` is not set (either in the configuration
file or on the command line). When an identity file is provided, the password
is used to decrypt it and the password authentication method is not tried with
the server. The only SSH authentication methods used are password and
publickey. If an SSH agent is available, it is always used.

When set to `gcs`, files are uploaded to Google Cloud Storage. The `--gcs-*`
family of options can be used to setup access to the bucket. When `--gcs-keyfile`
is empty, `GOOGLE_APPLICATION_CREDENTIALS` environment is used.

When set to `azure`, files are uploaded to Azure Blob Storage. The `--azure-*`
family of options can be used to setup access to the container. The name of the
container is mandatory. If the account name is left empty, an anonymous
connection is used and the endpoint is used directly: this allows the use of a
full URL to the container with a SAS token. When an account is provided, the
URL is built by prepending the container name to the endpoint and scheme is
always `https`. The default endpoint is `blob.core.windows.net`. The
`AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` are used when `--azure-account`
and `--azure-key` are not set (on the command line or corresponding options in
the configuration file).

The `--purge-remote` option can be set to `yes` to apply the same purge policy
on the remote location as the local directory.

When files are encrypted and their unencrypted source is kept, only encrypted
files are uploaded.

## Restoring files

The following files are created:

* `pg_globals_{date}.sql`: definition of roles and tablespaces, dumped with
  `pg_dumpall -g`. This file is restored with `psql`.
* `pg_settings_{date}.out`: the list of server parameters found in the
  configuration files (9.5+) or in the `pg_settings` view. They shall be put
  back by hand.
* `ident_file_{date}.out`: the full contents of the `pg_ident.conf` file,
  usually located in the data directory.
* `hba_file_{date}.out`: the full contents of the `pg_hba.conf` file, usually
  located in the data directory.
* `{dbname}_{date}.createdb.sql`: an SQL file containing the definition of the
  database and parameters set at the database or "role in database" level. It
  is mostly useful when using a version of `pg_dump` older than 11. It is
  restored with `psql`.
* `{dbname}_{date}.{d,sql,dump,tar}`: the dump of the database, with a suffix
  depending of its format. If the format is plain, the dump is suffixed with
  `sql` and must be restored with `psql`. Otherwise, it must be restored with
  `pg_restore`.

When checksum are computed, for each file described above, a text file of the
same name with a suffix naming the checksum algorithm is produced.

When files are encrypted, they are suffixed with `age` and must be decrypted
first, see the [Encryption] section above. When checksums are computed and
encryption is required, checksum files are encrypted and encrypted files are
checksummed.

To sum up, when restoring:

1. Create the roles and tablespaces by executing `pg_globals_{date}.sql` with `psql`.
2. Create the database with `{dbname}_{date}.createdb.sql` if necessary.
3. Restore the database(s) with `pg_restore` (use `-C` to create the database) or `psql`


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
