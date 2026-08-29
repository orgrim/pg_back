Initialize and start a PostgreSQL instance:
  $ mkdir $TMPDIR/pg
  $ mkdir $TMPDIR/logs

  $ /usr/lib/postgresql/18/bin/initdb $TMPDIR/pg \
  >   > $TMPDIR/logs/initdb.log \
  >   2> $TMPDIR/logs/initdb.log

  $ /usr/lib/postgresql/18/bin/pg_ctl -D $TMPDIR/pg \
  >   -l $TMPDIR/logs/pg.log start \
  >   -o "-c unix_socket_directories='$TMPDIR'" > /dev/null 2>&1

Make sure we stop the PostgreSQL instance at teardown

  $ trap "/usr/lib/postgresql/18/bin/pg_ctl -D $TMPDIR/pg stop > /dev/null 2>&1" EXIT

Load test data

  $ psql -h $TMPDIR postgres -f $TESTDIR/testdata/fixture.sql
  CREATE ROLE
  CREATE DATABASE
  ALTER ROLE
  ALTER ROLE
  ALTER DATABASE
  ALTER DATABASE
  CREATE ROLE
  CREATE DATABASE
  REVOKE
  GRANT
  You are now connected to database "b1" as user "*". (glob)
  SET
  SELECT 10
  SELECT 10
  SELECT 10
  SELECT 10
  You are now connected to database "b2" as user "*". (glob)
  GRANT
  SET
  SELECT 10
  SELECT 10
  SELECT 10
  SELECT 10

And finally execute the tests

  $ export PGBK_TEST_CONNINFO="host=${TMPDIR} port=5432 dbname=postgres user=postgres"

First determine if help message can be displayed:

  $ pg_back --help
  pg_back dumps some PostgreSQL databases
  
  Usage:
    pg_back [OPTION]... [DBNAME]...
  
  Options:
    -c, --config string                   alternate config file (default "/etc/pg_back/pg_back.conf")
        --no-config-file                  skip reading config file
                                          
    -B, --bin-directory string            PostgreSQL binaries directory. Empty to search $PATH
    -b, --backup-directory string         store dump files there (default "/var/backups/postgresql")
    -m, --backup-file-mode string         mode to apply to dump files (default "0600")
    -D, --exclude-dbs strings             list of databases to exclude
    -t, --with-templates                  include templates
        --without-templates               force exclude templates
        --with-role-passwords             dump globals with role passwords (default true)
        --without-role-passwords          do not dump passwords of roles
        --dump-only                       only dump databases, excluding configuration and globals
    -T, --pause-timeout int               abort if replication cannot be paused after this number
                                          of seconds (default 3600)
    -j, --jobs int                        dump this many databases concurrently (default 1)
    -F, --format string                   database dump format: plain, custom, tar or directory (default "custom")
    -J, --parallel-backup-jobs int        number of parallel jobs to dumps when using directory format (default 1)
    -Z, --compress int                    compression level for compressed formats (default -1)
    -S, --checksum-algo string            signature algorithm: none sha1 sha224 sha256 sha384 sha512 (default "none")
        --uniform-timestamp               Use the same timestamp for all pg_back files instead of individual
                                          creation times
    -P, --purge-older-than string         purge backups older than this duration in days
                                          use an interval with units "s" (seconds), "m" (minutes) or "h" (hours)
                                          for less than a day. (default "30")
    -K, --purge-min-keep string           minimum number of dumps to keep when purging or 'all' to keep
                                          everything (default "0")
        --pre-backup-hook string          command to run before taking dumps
        --post-backup-hook string         command to run after taking dumps
                                          
        --encrypt                         encrypt the dumps
        --no-encrypt                      do not encrypt the dumps
        --encrypt-keep-src                keep original files when encrypting
        --no-encrypt-keep-src             do not keep original files when encrypting
        --decrypt                         decrypt files in the backup directory instead of dumping. DBNAMEs become
                                          globs to select files
        --cipher-pass string              cipher passphrase for encryption and decryption
        --cipher-public-key string        AGE public key for encryption; in Bech32 encoding starting with 'age1'
        --cipher-private-key string       AGE private key for decryption; in Bech32 encoding starting with
                                          'AGE-SECRET-KEY-1'
                                          
        --upload string                   upload produced files to target (s3, gcs,..) use "none" to override
                                          configuration file and disable upload (default "none")
        --upload-prefix string            add this prefix to uploaded files, similar to a target directory
        --delete-uploaded string          delete local file after upload (default "no")
        --download string                 download files from target (s3, gcs,..) instead of dumping. DBNAMEs become
                                          globs to select files (default "none")
        --list-remote string              list the remote files on s3, gcs, sftp, azure instead of dumping. DBNAMEs
                                          become globs to select files (default "none")
        --purge-remote string             purge the file on remote location after upload, with the same rules
                                          as the local directory (default "no")
        --b2-bucket string                Backblaze B2 bucket
        --b2-key-id string                Backblaze B2 access key ID
        --b2-app-key string               Backblaze B2 app key
        --b2-force-path string            force path style addressing instead of virtual hosted bucket
                                          addressing (default "no")
        --b2-concurrent-connections int   set the amount of concurrent b2 http connections (default 5)
        --s3-region string                S3 region
        --s3-bucket string                S3 bucket
        --s3-profile string               AWS client profile name to get credentials
        --s3-key-id string                AWS Access key ID
        --s3-secret string                AWS Secret access key
        --s3-endpoint string              S3 endpoint URI
        --s3-force-path string            force path style addressing instead of virtual hosted bucket
                                          addressing (default "no")
        --s3-tls string                   enable or disable TLS on requests (default "yes")
        --sftp-host string                Remote hostname for SFTP
        --sftp-port string                Remote port for SFTP
        --sftp-user string                Login for SFTP when different than the current user
        --sftp-password string            Password for SFTP or passphrase when identity file is set
        --sftp-directory string           Target directory on the remote host
        --sftp-identity string            Path to a private key
        --sftp-ignore-hostkey string      Check the target host key against local known hosts (default "no")
        --gcs-bucket string               GCS bucket name
        --gcs-endpoint string             GCS endpoint URL
        --gcs-keyfile string              path to the GCS credentials file
        --azure-container string          Azure Blob Container
        --azure-account string            Azure Blob Storage account
        --azure-key string                Azure Blob Storage shared key
        --azure-endpoint string           Azure Blob Storage endpoint (default "blob.core.windows.net")
    -h, --host string                     database server host or socket directory
    -p, --port int                        database server port number
    -U, --username string                 connect as specified database user
    -d, --dbname string                   connect to database name
                                          
        --convert-legacy-config string    convert a pg_back v1 configuration file
        --print-default-config            print the default configuration
                                          
    -q, --quiet                           quiet mode
    -v, --verbose                         verbose mode
                                          
    -?, --help                            print usage
    -V, --version                         print version

Then try to take a first backup (${TMPDIR}/backups directory is created):
  $ test -d $TMPDIR/backups || echo "backups directory does not exist yet"
  backups directory does not exist yet

  $ export PGBK_TEST_CONNINFO="host=${TMPDIR} port=5432 dbname=postgres user=postgres"
  $ pg_back --host ${TMPDIR} --port 5432 --dbname postgres --backup-directory=${TMPDIR}/backups
  * INFO: dumping globals (glob)
  * INFO: dumping instance configuration (glob)
  * INFO: dumping database postgres (glob)
  * INFO: dump of postgres to $TMPDIR/backups/postgres_*.dump done (glob)
  * INFO: dumping database b1 (glob)
  * INFO: dump of b1 to $TMPDIR/backups/b1_*.dump done (glob)
  * INFO: dumping database b2 (glob)
  * INFO: dump of b2 to $TMPDIR/backups/b2_20*.dump done (glob)
  * INFO: waiting for postprocessing to complete (glob)
  * INFO: purging old dumps (glob)

  $ test -d $TMPDIR/backups && echo "backups directory has been created"
  backups directory has been created

List backups:
  $ find $TMPDIR/backups
  $TMPDIR/backups
  $TMPDIR/backups/b2_*.dump (glob)
  $TMPDIR/backups/b1_*.dump (glob)
  $TMPDIR/backups/postgres_*.dump (glob)
  $TMPDIR/backups/ident_file_*.out (glob)
  $TMPDIR/backups/hba_file_*.out (glob)
  $TMPDIR/backups/pg_settings_*.out (glob)
  $TMPDIR/backups/pg_globals_*.sql (glob)

Take another backup:
  $ pg_back --host ${TMPDIR} --port 5432 --dbname postgres \
  >   --backup-directory=${TMPDIR}/backups
  * INFO: dumping globals (glob)
  * INFO: dumping instance configuration (glob)
  * INFO: dumping database postgres (glob)
  * INFO: dump of postgres to $TMPDIR/backups/postgres_*.dump done (glob)
  * INFO: dumping database b1 (glob)
  * INFO: dump of b1 to $TMPDIR/backups/b1_*.dump done (glob)
  * INFO: dumping database b2 (glob)
  * INFO: dump of b2 to $TMPDIR/backups/b2_20*.dump done (glob)
  * INFO: waiting for postprocessing to complete (glob)
  * INFO: purging old dumps (glob)
  $ pg_back --host ${TMPDIR} --port 5432 --dbname postgres \
  >   --backup-directory=${TMPDIR}/backups --purge-min-keep 1 \
  >   --purge-older-than 1s --parallel-backup-jobs 2
  * INFO: dumping globals (glob)
  * INFO: dumping instance configuration (glob)
  * INFO: dumping database postgres (glob)
  * INFO: dump of postgres to $TMPDIR/backups/postgres_*.dump done (glob)
  * INFO: dumping database b1 (glob)
  * INFO: dump of b1 to $TMPDIR/backups/b1_*.dump done (glob)
  * INFO: dumping database b2 (glob)
  * INFO: dump of b2 to $TMPDIR/backups/b2_20*.dump done (glob)
  * INFO: waiting for postprocessing to complete (glob)
  * INFO: purging old dumps (glob)
