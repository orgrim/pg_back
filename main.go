// pg_back
//
// Copyright 2011-2021 Nicolas Thauvin and contributors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  1. Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//  2. Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in the
//     documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHORS ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	_ "embed"

	pgcommand "github.com/orgrim/pg_back/internal/command"
	"github.com/orgrim/pg_back/internal/config"
	"github.com/orgrim/pg_back/internal/crypto"
	"github.com/orgrim/pg_back/internal/helpers"
	"github.com/orgrim/pg_back/internal/legacy"
	"github.com/orgrim/pg_back/internal/logger"
	"github.com/orgrim/pg_back/internal/postgresql"
	"github.com/orgrim/pg_back/internal/storage"
)

//go:embed pg_back.conf
var defaultCfg string

var binDir string
var l = logger.NewLevelLog()

type dump struct {
	// Name of the database to dump
	Database string

	// Per database pg_dump options to filter schema, tables, etc.
	Options *config.DbOpts

	// Path is the output file or directory of the dump
	// a directory is output with the directory format of pg_dump
	// It remains empty until after the dump is done
	Path string

	// Directory is the target directory where to create the dump
	Directory string

	// Mode is the permission for the resulting backup
	Mode int

	// Time format for the filename
	TimeFormat string

	// Connection parameters
	ConnString *postgresql.ConnInfo

	// Cipher passphrase, when not empty cipher the file
	CipherPassphrase string

	// AGE public key used for encryption; in Bech32 encoding starting with "age1"
	CipherPublicKey string

	// Keep original files after encryption
	EncryptKeepSrc bool

	// Result
	When     time.Time
	ExitCode int

	// Version of pg_dump
	PgDumpVersion int
}

func main() {
	// Use another function to allow the use of defer for cleanup, as
	// os.Exit() does not run deferred functions
	if err := run(); err != nil {
		l.Fatalln(err)
		os.Exit(1)
	}
}

func run() (retVal error) {
	// Parse commanline arguments first so that we can quit if we
	// have shown usage or version string. We may have to load a
	// non default configuration file
	cliOpts, cliOptList, err := config.ParseCli(os.Args[1:], defaultCfg)
	var pce *config.ParseCliResult
	if err != nil {
		if errors.As(err, &pce) {
			// Convert the configuration file if a path as been
			// passed in the result and exit. Since the
			// configuration file from pg_back v1 is a shell
			// script, we may just fail to convert it. So we just
			// output the result on stdout and exit to let the user
			// check the result
			if len(pce.LegacyConfig) > 0 {
				if err := legacy.ConvertLegacyConfFile(l, pce.LegacyConfig); err != nil {
					return err
				}
			}
			return nil
		}

		return err
	}

	// Enable verbose mode or quiet mode as soon as possible
	l.SetVerbosity(cliOpts.Verbose, cliOpts.Quiet)

	var cliOptions config.Options

	if cliOpts.NoConfigFile {
		l.Infoln("Skipping reading config file")
		cliOptions = config.DefaultOptions()
	} else {
		// Load configuration file and allow the default configuration
		// file to be absent
		cliOptions, err = config.LoadConfigurationFile(cliOpts.CfgFile, l)
		if err != nil {
			return err
		}
	}

	// override options from the configuration file with ones from
	// the command line
	opts := config.MergeCliAndConfigOptions(cliOpts, cliOptions, cliOptList)

	err = ensureCipherParamsPresent(&opts)
	if err != nil {
		return fmt.Errorf("required cipher parameters not present: %w", err)
	}

	if (opts.Upload == "s3" || opts.Download == "s3" || opts.ListRemote == "s3") &&
		opts.S3Bucket == "" {
		return fmt.Errorf("a bucket is mandatory with s3")
	}

	if (opts.Upload == "b2" || opts.Download == "b2" || opts.ListRemote == "b2") &&
		opts.B2Bucket == "" {
		return fmt.Errorf("a bucket is mandatory with B2")
	}

	if (opts.Upload == "gcs" || opts.Download == "gcs" || opts.ListRemote == "gcs") &&
		opts.GCSBucket == "" {
		return fmt.Errorf("a bucket is mandatory with gcs")
	}

	if (opts.Upload == "azure" || opts.Download == "azure" || opts.ListRemote == "azure") &&
		opts.AzureContainer == "" {
		return fmt.Errorf("a container is mandatory with azure")
	}

	// Run actions that won't dump databases first, in that case the list
	// of databases become file globs.  Avoid getting wrong globs from the
	// config file since we are using the remaining args from the command
	// line that are usually as a list of databases to dump
	globs := []string{}
	if slices.Contains(cliOptList, "include-dbs") {
		globs = opts.Dbnames
	}

	// Listing remote files take priority over the other options that won't dump databases
	if opts.ListRemote != "none" {
		if err := listRemoteFiles(opts.ListRemote, opts, globs); err != nil {
			return err
		}

		return nil
	}

	// When asked to download or decrypt the backups, do it here and exit, we have all
	// required input (passphrase and backup directory)
	if opts.Decrypt || opts.Download != "none" {
		if opts.Download != "none" {
			if err := downloadFiles(opts.Download, opts, opts.Directory, globs); err != nil {
				return err
			}
		}

		if opts.Decrypt {
			params := crypto.DecryptParams{
				Logger:     l,
				PrivateKey: opts.CipherPrivateKey,
				Passphrase: opts.CipherPassphrase,
			}
			if err := decryptDirectory(opts.Directory, params, opts.Jobs, globs); err != nil {
				return err
			}
		}

		return nil
	}

	// Remember when we start so that a purge interval of 0s won't remove
	// the dumps we are taking. We truncate the time to the second because
	// the purge parses the date in the name of the file and its resolution
	// is the second, thus the parsing truncates to the second.
	now := time.Now().Truncate(time.Second)

	if opts.BinDirectory != "" {
		binDir = opts.BinDirectory
	}

	// Ensure that pg_dump accepts the options we will give it
	pgDumpVersion := pgcommand.PgToolVersion(l, binDir, "pg_dump")

	if pgDumpVersion < 80400 {
		return fmt.Errorf("provided pg_dump is older than 8.4, unable use it")
	}

	// Parse the connection information
	l.Verboseln("processing input connection parameters")
	conninfo, err := postgresql.PrepareConnInfo(opts.Host, opts.Port, opts.Username, opts.ConnDb)
	if err != nil {
		return fmt.Errorf("could not compute connection string: %w", err)
	}
	if conninfo.Infos["application_name"] == "pg_back" {
		l.Verboseln("using pg_back as application_name")
	}

	defer pgcommand.PostBackupHook(l, opts.PostHook)
	if err := pgcommand.PreBackupHook(l, opts.PreHook); err != nil {
		return err
	}

	// Use another goroutine to compute checksum and other operations on
	// files by sending them with this channel
	producedFiles := make(chan sumFileJob)

	var wg sync.WaitGroup

	postProcRet := postProcessFiles(producedFiles, &wg, opts)

	// retVal allow us to return with an error from the post processing go
	// routines, by changing it in a deferred function. Using deferred
	// function helps preventing us from forgetting any cleanup task. This
	// is why retVal is named in the signature of run().
	defer func() {
		// Detect if the producedFiles channel has been closed twice,
		// it will be closed to stop post processing and check for
		// error before purging old files
		if err := recover(); err == nil {
			l.Infoln("waiting for postprocessing to complete")
		}

		err := stopPostProcess(&wg, postProcRet)
		if err != nil {
			if retVal != nil {
				// Do not overwrite the error
				l.Errorln("failed to stop postprocessing:", err)
			} else {
				retVal = err
			}
		}
	}()

	// Closing the input channel makes the postprocessing go routine stop,
	// so it must be done before blocking on the WaitGroup in stopPostProcess()
	defer close(producedFiles)

	// Connect before running pg_dumpall so that we know if the user is superuser
	db, err := postgresql.DbOpen(l, conninfo)
	if err != nil {
		return fmt.Errorf("connection to PostgreSQL failed: %w", err)
	}
	defer helpers.WrappedClose(db, &retVal)

	// Generate a single datetime that will be used in all files generated by pg_back
	var fileTime time.Time
	if opts.UniformTimestamp {
		fileTime = time.Now()
	}

	if !opts.DumpOnly {
		if !db.Superuser {
			l.Infoln("connection user is not superuser, some information will not be dumped")
		}

		// Then we can implicitely avoid dumping role password when using a
		// regular user
		dumpRolePasswords := opts.WithRolePasswords && db.Superuser
		if dumpRolePasswords {
			l.Infoln("dumping globals")
		} else {
			l.Infoln("dumping globals without role passwords")
		}
		if err := dumpGlobals(opts.Directory, opts.Mode, opts.TimeFormat, dumpRolePasswords, conninfo, producedFiles, fileTime); err != nil {
			return fmt.Errorf("pg_dumpall of globals failed: %w", err)
		}

		l.Infoln("dumping instance configuration")
		var (
			verr *postgresql.PgVersionError
			perr *postgresql.PgPrivError
		)

		if err := dumpSettings(opts.Directory, opts.Mode, opts.TimeFormat, db, producedFiles, fileTime); err != nil {
			if errors.As(err, &verr) || errors.As(err, &perr) {
				l.Warnln(err)
			} else {
				return fmt.Errorf("could not dump configuration parameters: %w", err)
			}
		}

		if err := dumpConfigFiles(opts.Directory, opts.Mode, opts.TimeFormat, db, producedFiles, fileTime); err != nil {
			return fmt.Errorf("could not dump configuration files: %w", err)
		}
	}

	databases, err := postgresql.ListDatabases(
		l,
		db,
		opts.WithTemplates,
		opts.ExcludeDbs,
		opts.Dbnames,
	)
	if err != nil {
		return err
	}
	l.Verboseln("databases to dump:", databases)

	if err := postgresql.PauseReplicationWithTimeout(l, db, opts.PauseTimeout); err != nil {
		return err
	}

	exitCode := 0
	maxWorkers := opts.Jobs
	numJobs := len(databases)
	jobs := make(chan *dump, numJobs)
	results := make(chan *dump, numJobs)

	// start workers - thanks gobyexample.com
	l.Verbosef("launching %d workers", maxWorkers)
	for range maxWorkers {
		go dumper(jobs, results, producedFiles)
	}

	defDbOpts := defaultDbOpts(opts)

	var passphrase, publicKey string
	if opts.Encrypt {
		passphrase = opts.CipherPassphrase
		publicKey = opts.CipherPublicKey
	}

	// feed the database
	for _, dbname := range databases {
		o, found := opts.PerDbOpts[dbname]
		if !found {
			o = defDbOpts
		}

		d := &dump{
			Database:         dbname,
			Options:          o,
			Directory:        opts.Directory,
			Mode:             opts.Mode,
			TimeFormat:       opts.TimeFormat,
			ConnString:       conninfo,
			CipherPassphrase: passphrase,
			CipherPublicKey:  publicKey,
			EncryptKeepSrc:   opts.EncryptKeepSrc,
			When:             fileTime,
			ExitCode:         -1,
			PgDumpVersion:    pgDumpVersion,
		}

		l.Verbosef("sending dump job for database %s to worker pool", dbname)
		jobs <- d
	}

	canDumpACL := true
	canDumpConfig := true

	// When asked to only dump database, exclude ACL and config even if
	// this can lead of missing info on restore when pg_dump is older than
	// 11
	if opts.DumpOnly {
		canDumpACL = false
		canDumpConfig = false
	}

	// collect the result of the jobs
	for range numJobs {
		var b, c string
		var err error

		l.Verboseln("waiting for worker to send job back")
		d := <-results
		dbname := d.Database
		l.Verboseln("received job result of", dbname)
		if d.ExitCode > 0 {
			exitCode = 1
		}

		// Dump the ACL and Configuration of the
		// database. Since the information is in the catalog,
		// if it fails once it fails all the time.
		if canDumpACL {
			l.Verboseln("dumping create database query and ACL of", dbname)
			force := false
			if d.Options.Format == 'p' {
				force = true
			}

			b, err = postgresql.DumpCreateDBAndACL(l, db, dbname, pgDumpVersion, force)
			var verr *postgresql.PgVersionError
			if err != nil {
				if !errors.As(err, &verr) {
					l.Errorln(err)
					exitCode = 1
				} else {
					l.Warnln(err)
					canDumpACL = false
				}
			}
		}

		if canDumpConfig {
			l.Verboseln("dumping configuration of", dbname)
			c, err = postgresql.DumpDBConfig(l, db, dbname, pgDumpVersion)
			if err != nil {
				var verr *postgresql.PgVersionError
				if !errors.As(err, &verr) {
					l.Errorln(err)
					exitCode = 1
				} else {
					l.Warnln(err)
					canDumpConfig = false
				}
			}
		}

		// Write ACL and configuration to an SQL file
		if len(b) > 0 || len(c) > 0 {

			aclpath := helpers.FormatDumpPath(
				d.Directory,
				d.TimeFormat,
				"createdb.sql",
				dbname,
				d.When,
				0,
			)
			if err := os.MkdirAll(filepath.Dir(aclpath), 0700); err != nil {
				l.Errorln(err)
				exitCode = 1
				continue
			}

			f, err := os.Create(aclpath)
			if err != nil {
				l.Errorln(err)
				exitCode = 1
				continue
			}

			if _, err := fmt.Fprintf(f, "%s", b); err != nil {
				return fmt.Errorf("could not write to %s: %w", aclpath, err)
			}
			if _, err := fmt.Fprintf(f, "%s", c); err != nil {
				return fmt.Errorf("could not write to %s: %w", aclpath, err)
			}

			if err := f.Close(); err != nil {
				return fmt.Errorf("could not close %s: %w", aclpath, err)
			}
			if err := os.Chmod(aclpath, os.FileMode(d.Mode)); err != nil {
				return fmt.Errorf(
					"could not chmod to more secure permission for ACL %s: %w",
					dbname,
					err,
				)
			}

			// Have its checksum computed
			producedFiles <- sumFileJob{
				Path:    aclpath,
				SumAlgo: d.Options.SumAlgo,
			}

			l.Infoln("dump of ACL and configuration of", dbname, "to", aclpath, "done")
		}
	}

	if err := postgresql.ResumeReplication(l, db); err != nil {
		l.Errorln(err)
	}
	if err := db.Close(); err != nil {
		l.Errorln(err)
	}

	if exitCode != 0 {
		return fmt.Errorf("some operation failed")
	}

	// Closing the input channel makes the postprocessing go routine stop,
	// so it must be done before blocking on the WaitGroup in
	// stopPostProcess()
	close(producedFiles)
	l.Infoln("waiting for postprocessing to complete")
	if err := stopPostProcess(&wg, postProcRet); err != nil {
		return err
	}

	// purge old dumps per database and treat special files
	// (globals and settings) like databases
	l.Infoln("purging old dumps")

	var repo storage.Repo

	switch opts.Upload {
	case "s3":
		repo, err = storage.NewS3Repo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload to S3: %w", err)
		}
	case "b2":
		repo, err = storage.NewB2Repo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload to B2: %w", err)
		}
	case "sftp":
		repo, err = storage.NewSFTPRepo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload over SFTP: %w", err)
		}
	case "gcs":
		repo, err = storage.NewGCSRepo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload to GCS: %w", err)
		}
	case "azure":
		repo, err = storage.NewAzRepo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload to Azure: %w", err)
		}
	}

	for _, dbname := range databases {
		o, found := opts.PerDbOpts[dbname]
		if !found {
			o = defDbOpts
		}
		limit := now.Add(o.PurgeInterval)

		if err := storage.PurgeDumps(l, opts.Directory, dbname, o.PurgeKeep, limit); err != nil {
			retVal = err
		}

		if opts.PurgeRemote && repo != nil {
			if err := storage.PurgeRemoteDumps(l, repo, opts.UploadPrefix, opts.Directory, dbname, o.PurgeKeep, limit); err != nil {
				retVal = err
			}
		}
	}

	if !opts.DumpOnly {
		for _, other := range []string{"pg_globals", "pg_settings", "hba_file", "ident_file"} {
			limit := now.Add(defDbOpts.PurgeInterval)
			if err := storage.PurgeDumps(l, opts.Directory, other, defDbOpts.PurgeKeep, limit); err != nil {
				retVal = err
			}

			if opts.PurgeRemote && repo != nil {
				if err := storage.PurgeRemoteDumps(l, repo, opts.UploadPrefix, opts.Directory, other, defDbOpts.PurgeKeep, limit); err != nil {
					retVal = err
				}
			}
		}
	}

	return
}

func defaultDbOpts(opts config.Options) *config.DbOpts {
	dbo := config.DbOpts{
		Format:        opts.Format,
		Jobs:          opts.DirJobs,
		CompressLevel: opts.CompressLevel,
		SumAlgo:       opts.SumAlgo,
		PurgeInterval: opts.PurgeInterval,
		PurgeKeep:     opts.PurgeKeep,
		PgDumpOpts:    opts.PgDumpOpts,
		Username:      opts.Username,
	}
	return &dbo
}

func (d *dump) dump(fc chan<- sumFileJob) error {
	dbname := d.Database
	d.ExitCode = 1

	l.Infoln("dumping database", dbname)

	// Try to lock a file named after to database we are going to
	// dump to prevent stacking pg_back processes if pg_dump last
	// longer than a schedule of pg_back. If the lock cannot be
	// acquired, skip the dump and exit with an error.
	lock := helpers.FormatDumpPath(d.Directory, d.TimeFormat, "lock", dbname, time.Time{}, 0)
	flock, locked, err := storage.LockPath(l, lock)
	if err != nil {
		return fmt.Errorf("unable to lock %s: %s", lock, err)
	}

	if !locked {
		return fmt.Errorf("could not acquire lock for %s", dbname)
	}

	var fileEnd string
	switch d.Options.Format {
	case 'p':
		fileEnd = "sql"
	case 'c':
		fileEnd = "dump"
	case 't':
		fileEnd = "tar"
	case 'd':
		if d.PgDumpVersion < 90100 {
			return fmt.Errorf("provided pg_dump version does not support directory format")
		}

		fileEnd = "d"
	}

	if d.When.IsZero() {
		d.When = time.Now()
	}

	file := helpers.FormatDumpPath(
		d.Directory,
		d.TimeFormat,
		fileEnd,
		dbname,
		d.When,
		d.Options.CompressLevel,
	)
	formatOpt := fmt.Sprintf("-F%c", d.Options.Format)

	command := pgcommand.ExecPath(binDir, "pg_dump")
	args := []string{formatOpt, "-f", file, "-w"}

	if fileEnd == "d" && d.Options.Jobs > 1 {
		if d.PgDumpVersion < 90300 {
			l.Warnln("provided pg_dump version does not support parallel jobs, ignoring option")
		} else {
			args = append(args, "-j", fmt.Sprintf("%d", d.Options.Jobs))
		}
	}

	// Included and excluded schemas and table
	for _, obj := range d.Options.Schemas {
		args = append(args, "-n", obj)
	}
	for _, obj := range d.Options.ExcludedSchemas {
		args = append(args, "-N", obj)
	}
	for _, obj := range d.Options.Tables {
		args = append(args, "-t", obj)
	}
	for _, obj := range d.Options.ExcludedTables {
		args = append(args, "-T", obj)
	}

	switch d.Options.WithBlobs {
	case 1: // with blobs
		args = append(args, "-b")
	case 2: // without blobs
		if d.PgDumpVersion < 100000 {
			l.Warnln("provided pg_dump version does not support excluding blobs, ignoring option")
		} else {
			args = append(args, "-B")
		}
	}

	// Add compression level option only if not dumping in the tar format
	if d.Options.CompressLevel >= 0 {
		if d.Options.Format != 't' {
			args = append(args, "-Z", fmt.Sprintf("%d", d.Options.CompressLevel))
		} else {
			l.Warnln("compression level is not supported by the target format")
		}
	}

	if len(d.Options.PgDumpOpts) > 0 {
		args = append(args, d.Options.PgDumpOpts...)
	}

	// Connection option are passed as a connstring even if we add options
	// on the command line. For older version, it is passed using the
	// environment
	conninfo := d.ConnString.Set("dbname", dbname)
	if d.Options.Username != "" {
		conninfo = conninfo.Set("user", d.Options.Username)
	}

	var env []string

	if d.PgDumpVersion < 90300 {
		args = append(args, dbname)
		env = os.Environ()
		env = append(env, d.ConnString.MakeEnv()...)
	} else {
		args = append(args, "-d", conninfo.String())
	}

	pgDumpCmd := exec.Command(command, args...)
	pgDumpCmd.Env = env
	l.Verboseln("running:", pgDumpCmd)
	stdoutStderr, err := pgDumpCmd.CombinedOutput()
	if err != nil {
		for line := range strings.SplitSeq(string(stdoutStderr), "\n") {
			if line != "" {
				l.Errorf("[%s] %s\n", dbname, line)
			}
		}
		if err := storage.UnlockPath(l, flock); err != nil {
			l.Errorf("could not release lock for %s: %s", dbname, err)
			flock.Close() //nolint:errcheck
		}
		return err
	}
	if len(stdoutStderr) > 0 {
		for line := range strings.SplitSeq(string(stdoutStderr), "\n") {
			if line != "" {
				l.Infof("[%s] %s\n", dbname, line)
			}
		}
	}

	if err := storage.UnlockPath(l, flock); err != nil {
		flock.Close() //nolint:errcheck
		return fmt.Errorf("could not release lock for %s: %s", dbname, err)
	}

	// Send the info on the file for post processing
	if fc != nil {
		fc <- sumFileJob{
			Path:    file,
			SumAlgo: d.Options.SumAlgo,
		}
	}

	d.Path = file
	d.ExitCode = 0

	if d.Mode > 0 {
		var mode = os.FileMode(d.Mode)
		isDirFormat := d.Options.Format == 'd'
		if isDirFormat {
			// calculate appropriate permission for parent directory, we need +x
			// permission to walk through the directory
			if (mode&0o400 > 0) || (mode&0o200 > 0) {
				mode = mode | 0o100
			}

			if (mode&0o040 > 0) || (mode&0o020 > 0) {
				mode = mode | 0o010
			}

			if (mode&0o004 > 0) || (mode&0o002 > 0) {
				mode = mode | 0o001
			}
		}

		if err := os.Chmod(file, mode); err != nil {
			return fmt.Errorf("could not chmod to more secure permission for %s: %w", dbname, err)
		}

		if isDirFormat {
			// adapt mode on files on directory based on initial configured mode
			if err := recursiveChmod(file, os.FileMode(d.Mode)); err != nil {
				return err
			}
		}
	}

	return nil
}

func recursiveChmod(file string, newMode os.FileMode) error {
	err := filepath.Walk(file, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error when accesing file %s: %w", path, err)
		}
		if !info.IsDir() {
			if err := os.Chmod(path, newMode); err != nil {
				return fmt.Errorf("could not chmod %s to more secure permission: %w", file, err)
			}
		}
		return nil

	})
	return err
}

func dumper(jobs <-chan *dump, results chan<- *dump, fc chan<- sumFileJob) {
	for j := range jobs {

		if err := j.dump(fc); err != nil {
			l.Errorln("dump of", j.Database, "failed:", err)
			results <- j
		} else {
			l.Infoln("dump of", j.Database, "to", j.Path, "done")
			results <- j
		}
	}
}

func ensureCipherParamsPresent(opts *config.Options) error {
	// Nothing needs to be done if we are not encrypting or decrypting
	if !opts.Encrypt && !opts.Decrypt {
		return nil
	}

	// If we are encrypting or decrypting, make sure we either have a public/private key or a passphrase
	needEncryptParams := opts.Encrypt && len(opts.CipherPublicKey) == 0 &&
		len(opts.CipherPassphrase) == 0
	needDecryptParams := opts.Decrypt && len(opts.CipherPrivateKey) == 0 &&
		len(opts.CipherPassphrase) == 0

	if needEncryptParams || needDecryptParams { // Fallback on the environment
		opts.CipherPassphrase = os.Getenv("PGBK_CIPHER_PASS")

		if len(opts.CipherPassphrase) == 0 {
			return fmt.Errorf("cannot use an empty passphrase for encryption")
		}
	}

	return nil
}

func dumpGlobals(
	dir string,
	mode int,
	timeFormat string,
	withRolePasswords bool,
	conninfo *postgresql.ConnInfo,
	fc chan<- sumFileJob,
	when time.Time,
) error {
	command := pgcommand.ExecPath(binDir, "pg_dumpall")
	args := []string{"-g", "-w"}

	// pg_dumpall only connects to another database if it is given
	// with the -l option
	if dbname, ok := conninfo.Infos["dbname"]; ok {
		args = append(args, "-l", dbname)
	}

	// With older version of PostgreSQL not supporting connection strings
	// on their -d option, use the environment to pass the connection
	// information
	var env []string

	pgDumpallVersion := pgcommand.PgToolVersion(l, binDir, "pg_dumpall")

	if pgDumpallVersion < 90300 {
		env = os.Environ()
		env = append(env, conninfo.MakeEnv()...)
	} else {
		args = append(args, "-d", conninfo.String())
	}

	// The --no-role-passwords option was added to pg_dumpall from 10
	if !withRolePasswords {
		if pgDumpallVersion < 100000 {
			return fmt.Errorf(
				"pg_dumpall does not support --no-role-passwords, use pg_dumpall >= 10",
			)
		}

		args = append(args, "--no-role-passwords")
	}

	if when.IsZero() {
		when = time.Now()
	}

	file := helpers.FormatDumpPath(dir, timeFormat, "sql", "pg_globals", when, 0)
	args = append(args, "-f", file)

	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		return err
	}

	pgDumpallCmd := exec.Command(command, args...)
	pgDumpallCmd.Env = env
	l.Verboseln("running:", pgDumpallCmd)
	stdoutStderr, err := pgDumpallCmd.CombinedOutput()
	if err != nil {
		for line := range strings.SplitSeq(string(stdoutStderr), "\n") {
			if line != "" {
				l.Errorln(line)
			}
		}
		return err
	}
	if len(stdoutStderr) > 0 {
		for line := range strings.SplitSeq(string(stdoutStderr), "\n") {
			if line != "" {
				l.Infoln(line)
			}
		}
	}
	if mode > 0 {
		if err := os.Chmod(file, os.FileMode(mode)); err != nil {
			return fmt.Errorf("could not chmod to more secure permission for pg_globals: %w", err)
		}
	}

	if fc != nil {
		fc <- sumFileJob{
			Path: file,
		}
	}

	return nil
}

func dumpSettings(
	dir string,
	mode int,
	timeFormat string,
	db *postgresql.Pg,
	fc chan<- sumFileJob,
	when time.Time,
) error {
	if when.IsZero() {
		when = time.Now()
	}

	file := helpers.FormatDumpPath(dir, timeFormat, "out", "pg_settings", when, 0)

	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		return err
	}

	s, err := postgresql.ShowSettings(l, db)
	if err != nil {
		return err
	}

	if len(s) > 0 {
		l.Verboseln("writing settings to:", file)
		f, err := os.Create(file)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(f, "%s", s); err != nil {
			return fmt.Errorf("could not write to file %s: %w", file, err)
		}
		if mode > 0 { // otherwhise let umask do the job
			if err := os.Chmod(f.Name(), os.FileMode(mode)); err != nil {
				return fmt.Errorf(
					"could not chmod to more secure permission for settings file %s: %w",
					f.Name(),
					err,
				)
			}
		}

		if fc != nil {
			fc <- sumFileJob{
				Path: file,
			}
		}
	}

	return nil
}

func dumpConfigFiles(
	dir string,
	mode int,
	timeFormat string,
	db *postgresql.Pg,
	fc chan<- sumFileJob,
	when time.Time,
) error {
	for _, param := range []string{"hba_file", "ident_file"} {
		if when.IsZero() {
			when = time.Now()
		}
		file := helpers.FormatDumpPath(dir, timeFormat, "out", param, when, 0)

		if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
			return err
		}

		s, err := postgresql.ExtractFileFromSettings(l, db, param)
		if err != nil {
			return err
		}

		// Use a Buffer to avoid creating an empty file
		if len(s) > 0 {
			l.Verbosef("writing contents of '%s' to: %s", param, file)
			f, err := os.Create(file)
			if err != nil {
				return err
			}

			if _, err := fmt.Fprintf(f, "%s", s); err != nil {
				return fmt.Errorf("could not write to file %s: %w", file, err)
			}

			if mode > 0 { // otherwhise let umask do the job
				if err := os.Chmod(f.Name(), os.FileMode(mode)); err != nil {
					return fmt.Errorf(
						"could not chmod to more secure permission for settings file %s: %w",
						f.Name(),
						err,
					)
				}
			}

			// We have produced a file send it to the channel for
			// further processing
			if fc != nil {
				fc <- sumFileJob{
					Path: file,
				}
			}
		}
	}
	return nil
}

func listRemoteFiles(repoName string, opts config.Options, globs []string) error {
	repo, err := storage.NewRepo(repoName, opts)
	if err != nil {
		return err
	}

	remoteFiles, err := repo.List(l, "")
	if err != nil {
		return fmt.Errorf("could not list contents of remote location: %w", err)
	}

	for _, i := range remoteFiles {
		keep := false
		if len(globs) == 0 {
			keep = true
		}

		for _, glob := range globs {
			keep, err = filepath.Match(glob, i.Key)
			if err != nil {
				return fmt.Errorf("bad patern: %w", err)
			}

			if keep {
				break
			}
		}

		if !keep {
			continue
		}

		fmt.Println(i.Key)
	}

	return nil
}

func downloadFiles(repoName string, opts config.Options, dir string, globs []string) error {
	repo, err := storage.NewRepo(repoName, opts)
	if err != nil {
		return err
	}

	// Without globs, there is nothing to download
	if len(globs) == 0 {
		return fmt.Errorf("no filter given to download files, use globs as command line arguments")
	}

	remoteFiles, err := repo.List(l, "")
	if err != nil {
		return fmt.Errorf("could not list contents of remote location: %w", err)
	}

	for _, i := range remoteFiles {
		keep := false
		for _, glob := range globs {
			keep, err = filepath.Match(glob, i.Key)
			if err != nil {
				return fmt.Errorf("bad patern: %w", err)
			}

			if keep {
				break
			}
		}

		if !keep {
			l.Verboseln("skipping:", i.Key)
			continue
		}

		if i.IsDir {
			l.Warnf(
				"%s is a directory, append %c* to the filter to download its contents",
				i.Key,
				os.PathSeparator,
			)
			continue
		}

		// Create any parent directory under target dir
		path := filepath.Join(dir, i.Key)
		parent := filepath.Dir(path)
		if err := os.MkdirAll(parent, 0700); err != nil {
			return fmt.Errorf("could not create directory %s: %w", parent, err)
		}

		if err := repo.Download(l, i.Key, path); err != nil {
			return err
		}
	}

	return nil
}

func decryptDirectory(dir string, params crypto.DecryptParams, workers int, globs []string) error {

	// Run a pool of workers to decrypt concurrently
	var wg sync.WaitGroup

	// Workers pick paths from the file queue
	fq := make(chan string)

	// We need a channel to know if a worker got an error at some point and
	// return an error
	ret := make(chan bool, workers)

	// Start workers that listen for filenames to decrypt until the queue
	// is closed
	for i := range workers {
		wg.Add(1)
		go func(id int) {
			l.Verboseln("started decrypt worker", id)
			failed := false
			for {
				file, more := <-fq
				if !more {
					break
				}

				l.Verbosef("[%d] processing: %s\n", id, file)
				if err := params.DecryptFile(file); err != nil {
					l.Errorln(err)
					failed = true
				}
			}

			if failed {
				ret <- true
			}

			wg.Done()
			l.Verboseln("terminated decrypt worker", id)
		}(i)
	}

	// Read the directory, filter the contents with the provided globs and
	// send the path to the workers. When a directory is found, send its
	// content, the first level only to the workers
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("unable to read directory %s: %w", dir, err)
	}

	var c int
	for _, path := range entries {
		keep := true
		if len(globs) > 0 {
			keep = false
			for _, glob := range globs {
				keep, err = filepath.Match(glob, path.Name())
				if err != nil {
					return fmt.Errorf("bad patern: %w", err)
				}

				if keep {
					break
				}
			}
		}

		if !keep {
			l.Verbosef("skipping: %s, patterns: %v\n", path.Name(), globs)
			continue
		}

		if path.IsDir() {
			l.Verboseln("dump is a directory, decrypting all files inside")
			subdir := filepath.Join(dir, path.Name())
			subentries, err := os.ReadDir(subdir)
			if err != nil {
				l.Warnf("unable to read subdir %s: %s", subdir, err)
				continue
			}

			for _, subpath := range subentries {
				if subpath.IsDir() {
					// skip garbage dir in dump directory
					continue
				}

				file := filepath.Join(subdir, subpath.Name())
				if strings.HasSuffix(file, ".age") {
					c++
					fq <- file
				}
			}
			continue
		}

		file := filepath.Join(dir, path.Name())
		if strings.HasSuffix(file, ".age") {
			c++
			fq <- file
		}
	}

	// Print a warning when no candidate files are found with a hint that the dbname is a glob
	if c == 0 {
		l.Warnln(
			"no candidate file found for decryption. Maybe add a wildcard (*) to the patterns?",
		)
	}

	// Closing the channel will make the workers stop as soon as it is
	// empty
	close(fq)
	wg.Wait()

	// Check the return channel to find if there was an error. There are
	// maybe more than one but when creating the buffered channel we can't
	// know the size of the buffer, so we limit to one error per worker to
	// avoid being blocked by channel
	select {
	case <-ret:
		return fmt.Errorf("failure in decrypt, please examine logs")
	default:
		return nil
	}
}

// All FileJobs struct store information on post processing that must be done
// on a file. Some go routine process one type of job and create the next job
// and send it to some other goroutine for that kind of job
type sumFileJob struct {
	// Path of the file or directory to checksum
	Path string

	// Checksum algorithm
	SumAlgo string
}

type encryptFileJob struct {
	// Path of the file or directory to checksum
	Path string

	// How to encrypt the file
	Params crypto.EncryptParams

	KeepSrc bool

	// Store the checksum algo here to pass it to sumEncryptFileJob jobs
	SumAlgo string
}

type sumEncryptFileJob struct {
	// List of path to process
	Paths []string

	// Checksum algorithm
	SumAlgo string

	SumFile string
}

type uploadJob struct {
	// Path to upload
	Path string
}

// postProcessFiles is the entrypoint for common tasks to perform on files
// produced during execution, checksum and encryption. Different go routines
// are spawn to process the files as soon as possible
func postProcessFiles(inFiles chan sumFileJob, wg *sync.WaitGroup, opts config.Options) chan error {
	// Create a channel for errors so that we can inform the main goroutine
	// that a job failed and have the program exit with a non-zero
	// status. This chan is buffered with the number of goroutines using it
	// here so that it never blocks, each go routine must send only one
	// error, other are only logged.
	ret := make(chan error, 4*opts.Jobs)

	// Create a channel so that each group of worker can tell their job is
	// done and the next group can be stopped
	done := make(chan bool)

	// The order of tasks (checksum, encryption, checksum of encrypted
	// files) is kept by passing jobs of different types to the next
	// goroutine over channels
	encIn := make(chan encryptFileJob)
	uploadIn := make(chan uploadJob)

	for i := range opts.Jobs {
		wg.Add(1)
		go func(id int) {
			l.Verboseln("started checksum worker", id)
			failed := false
			for {

				j, more := <-inFiles
				if !more {
					wg.Done()
					done <- true
					l.Verboseln("stopped checksum worker", id)
					return
				}

				// An empty checksum algorithm comes from function
				// operating at instance level, so we use the global
				// option value for them.
				if j.SumAlgo == "" {
					j.SumAlgo = opts.SumAlgo
				}

				if j.SumAlgo != "none" {
					l.Infoln("computing checksum of", j.Path)
					p, err := crypto.ChecksumFile(l, j.Path, opts.Mode, j.SumAlgo)
					if err != nil {
						l.Errorln("checksum failed:", err)
						if !failed {
							ret <- fmt.Errorf("checksum failed: %w", err)
							failed = true
						}
						continue
					}

					// send the checksum file to encryption or upload
					if opts.Encrypt {
						encIn <- encryptFileJob{
							Path: p,
							Params: crypto.EncryptParams{
								Logger:     l,
								Passphrase: opts.CipherPassphrase,
								PublicKey:  opts.CipherPublicKey,
							},
							KeepSrc: opts.EncryptKeepSrc,
							SumAlgo: j.SumAlgo,
						}
					} else if opts.Upload != "none" {
						// upload the checksum file only if it won't be encrypted
						uploadIn <- uploadJob{
							Path: p,
						}
					}

				}

				// send the file to the next step, encryption or upload
				if opts.Encrypt {
					encIn <- encryptFileJob{
						Path: j.Path,
						Params: crypto.EncryptParams{
							Logger:     l,
							Passphrase: opts.CipherPassphrase,
							PublicKey:  opts.CipherPublicKey,
						},
						KeepSrc: opts.EncryptKeepSrc,
						SumAlgo: j.SumAlgo,
					}
				} else if opts.Upload != "none" {
					// upload the file only if it won't be encrypted
					i, err := os.Stat(j.Path)
					if err != nil {
						l.Warnln(err)
						continue
					}

					if i.IsDir() {
						entries, err := os.ReadDir(j.Path)
						if err != nil {
							l.Warnf("unable to read directory %s: %s", j.Path, err)
							continue
						}

						for _, p := range entries {
							if p.IsDir() {
								// skip garbage dirs in dump directory
								continue
							}

							uploadIn <- uploadJob{
								Path: filepath.Join(j.Path, p.Name()),
							}
						}
					} else {

						uploadIn <- uploadJob{
							Path: j.Path,
						}
					}
				}
			}
		}(i)
	}

	sumEncIn := make(chan sumEncryptFileJob)

	for i := range opts.Jobs {
		wg.Add(1)
		go func(id int) {
			l.Verboseln("started encryption worker", id)
			failed := false
			for {
				j, more := <-encIn
				if !more {
					wg.Done()
					done <- true
					l.Verboseln("stopped encryption worker", id)
					return
				}

				if opts.Encrypt {
					l.Infoln("encrypting", j.Path)
					encFiles, err := j.Params.EncryptFile(j.Path, opts.Mode, j.KeepSrc)
					if err != nil {
						l.Errorln("encryption failed:", err)
						if !failed {
							ret <- fmt.Errorf("encryption failed: %w", err)
							failed = true
						}
						continue
					}

					// send the encrypted files to checksuming
					sumEncIn <- sumEncryptFileJob{
						Paths:   encFiles,
						SumAlgo: j.SumAlgo,
						SumFile: fmt.Sprintf("%s.age", j.Path),
					}

					// upload the encrypted files
					if opts.Upload != "none" {
						for _, p := range encFiles {
							uploadIn <- uploadJob{
								Path: p,
							}
						}
					}
				}
			}
		}(i)
	}

	for i := range opts.Jobs {
		wg.Add(1)
		go func(id int) {
			l.Verboseln("started checksum worker for encrypted files", id)
			failed := false
			for {
				j, more := <-sumEncIn
				if !more {
					wg.Done()
					done <- true
					l.Verboseln("stopped checksum worker for encrypted files", id)
					return
				}

				if j.SumAlgo == "" {
					j.SumAlgo = opts.SumAlgo
				}

				if j.SumAlgo != "none" {
					l.Infoln("computing checksum of", j.SumFile)
					p, err := crypto.ChecksumFileList(l, j.Paths, opts.Mode, j.SumAlgo, j.SumFile)
					if err != nil {
						l.Errorln("checksum of encrypted files failed:", err)
						if !failed {
							ret <- fmt.Errorf("checksum of encrypted files failed: %w", err)
							failed = true
						}
						continue
					}

					// upload the checksum file
					if opts.Upload != "none" {
						uploadIn <- uploadJob{
							Path: p,
						}
					}
				}
			}
		}(i)
	}

	repo, err := storage.NewRepo(opts.Upload, opts)
	if err != nil {
		l.Errorln(err)
		ret <- err
		repo = nil
	}

	for i := range opts.Jobs {
		wg.Add(1)
		go func(id int) {
			l.Verboseln("started upload worker", id)
			failed := false
			for {
				j, more := <-uploadIn
				if !more {
					wg.Done()
					done <- true
					l.Verboseln("stopped upload worker", id)
					return
				}

				if opts.Upload != "none" && repo != nil {
					// Prepend the global prefix to the relative path of the dump
					if err := repo.Upload(l, j.Path, filepath.Join(opts.UploadPrefix, helpers.RelPath(l, opts.Directory, j.Path))); err != nil {
						l.Errorln(err)
						if !failed {
							ret <- err
							failed = true
						}
						continue
					}

					l.Infoln("uploaded", j.Path)
					if opts.DeleteUploaded {
						l.Infoln("removing", j.Path)
						os.Remove(j.Path)
					}
				}
			}
		}(i)
	}

	// Start a goroutine to wait on each group and close the channel when
	// their job is done, it will sequentially tell the next group of
	// worker to stop.
	go func() {
		// inFiles will be closed outside of the function, when all
		// worker reading from it exit, close encIn to make the workers
		// reading from it stop, and so on.
		for range opts.Jobs {
			<-done
		}
		close(encIn)

		for range opts.Jobs {
			<-done
		}
		close(sumEncIn)

		for range opts.Jobs {
			<-done
		}
		close(uploadIn)

		for range opts.Jobs {
			<-done
		}

		if repo != nil {
			repo.Close() //nolint:errcheck
		}

	}()

	return ret
}

func stopPostProcess(wg *sync.WaitGroup, rc chan error) error {
	// Ensure the postprocessing is complete before check the
	// return channel, otherwise the select could miss it
	wg.Wait()

	select {
	case err := <-rc:
		return fmt.Errorf("some error encountered in postprocessing: %w", err)
	default:
	}

	return nil
}
