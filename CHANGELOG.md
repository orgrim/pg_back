# Changelog

## pg_back 2.0.1

* Use /var/run/postgresql as default host for connections
* Support Windows
* Force legacy timestamp format on Windows
* Allow postgresql URIs as connection strings
* Tell pg_dump and pg_dumpall never to prompt for a password

## pg_back 2.0.0

* Full rewrite in Go
* Better handling of configuration dump
* No need for pg_dumpacl anymore
* Long option names on the commandline
* New command line options:
  - --bin-directory - path to the binaries of PostgreSQL
  - --format - dump format
  - --parallel-backup-jobs - jobs for directory format
  - --compress - compression level for format that support it
  - --pre-backup-hook - command to run before backups
  - --post-backup-hook - command to run after backups
* keyword=value connection string support with the -d option
* Purge interval can be less than 1 day
* Allow concurrent pg_dump jobs
* Per database output directories using the {dbname} keyword in the path
* Per database configuration with schema and table inclusion/exclusion
* New configuration file format (ini) with an option to convert from the v1
  format
* RFC 3339 time format in file name by default
* Use semver for version numbers
* Add a set of unit tests

Incompatible changes from v1:

* Configuration file format: use --convert-legacy-config to convert a v1
  configuration file
* Fixed filename format with timestamp: either RFC 3339 or the default from v1
  (YYYY-mm-dd_HH-MM-SS)
* Hook commands are parsed and split respecting shell quotes, and passed to
  fork/exec, not a to shell


## pg_back 1.10

* Add signature in Directory format
* Allow negative integer for PGBK_PURGE


## pg_back 1.9

* Fix dumping settings not using connection parameters


## pg_back 1.8

* Add a timeout when trying to pause replication on standby clusters
* Add pre/post-backup hooks
* Save to output of SHOW ALL to a file, to backup parameters
* Add optionnal checksum of dump files
* New default configuration file path (/etc/pg_back/pg_back.conf) with
  backward compatibility
* Some bugfixes and improvements


## pg_back 1.7

* Fix the purge not handling pg_dumpacl SQL file properly
* Improve documentation of the configuration file


## pg_back 1.6

* Improvements on support for pg_dumpacl (0.1 and 0.2)


## pg_back 1.5

* Support for pg_dumpacl (https://github.com/dalibo/pg_dumpacl)
* RPM Packaging


## pg_back 1.4

* Support PostgreSQL 10


## pg_back 1.3

* Ensure replication is not paused while an exclusive lock is taken
* Allow a retention policy based on time and number of backups
* Allow to disable purge


## pg_back 1.2

* Support pausing replication on standby servers
* Add timestamped information messages


## pg_back 1.1

* Support the directory format of pg_dump


## pg_back 1.0

* First release

