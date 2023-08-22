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
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var version = "2.1.1"
var binDir string

type dump struct {
	// Name of the database to dump
	Database string

	// Per database pg_dump options to filter schema, tables, etc.
	Options *dbOpts

	// Path is the output file or directory of the dump
	// a directory is output with the directory format of pg_dump
	// It remains empty until after the dump is done
	Path string

	// Directory is the target directory where to create the dump
	Directory string

	// Time format for the filename
	TimeFormat string

	// Connection parameters
	ConnString *ConnInfo

	// Cipher passphrase, when not empty cipher the file
	CipherPassphrase string

	// Keep original files after encryption
	EncryptKeepSrc bool

	// Result
	When     time.Time
	ExitCode int

	// Version of pg_dump
	PgDumpVersion int
}

type dbOpts struct {
	// Format of the dump
	Format rune

	// Algorithm of the checksum of the file, "none" is used to
	// disable checksuming
	SumAlgo string

	// Number of parallel jobs for directory format
	Jobs int

	// Compression level for compressed formats, -1 means the default
	CompressLevel int

	// Purge configuration
	PurgeInterval time.Duration
	PurgeKeep     int

	// Limit schemas
	Schemas         []string
	ExcludedSchemas []string

	// Limit dumped tables
	Tables         []string
	ExcludedTables []string

	// Other pg_dump options to use
	PgDumpOpts []string

	// Whether to force the dump of large objects or not with pg_dump -b or
	// -B, or let pg_dump use its default. 0 means default, 1 include
	// blobs, 2 exclude blobs.
	WithBlobs int
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
	cliOpts, cliOptList, err := parseCli(os.Args[1:])
	var pce *parseCliResult
	if err != nil {
		if errors.As(err, &pce) {
			// Convert the configuration file if a path as been
			// passed in the result and exit. Since the
			// configuration file from pg_back v1 is a shell
			// script, we may just fail to convert it. So we just
			// output the result on stdout and exit to let the user
			// check the result
			if len(pce.LegacyConfig) > 0 {
				if err := convertLegacyConfFile(pce.LegacyConfig); err != nil {
					return err
				}
			}
			return nil
		}

		return err
	}

	// Enable verbose mode or quiet mode as soon as possible
	l.SetVerbosity(cliOpts.Verbose, cliOpts.Quiet)

	var cliOptions options

	if cliOpts.NoConfigFile {
		l.Infoln("*** Skipping reading config file")
		cliOptions = defaultOptions()
	} else {
		// Load configuration file and allow the default configuration
		// file to be absent
		cliOptions, err = loadConfigurationFile(cliOpts.CfgFile)
		if err != nil {
			return err
		}
	}

	// override options from the configuration file with ones from
	// the command line
	opts := mergeCliAndConfigOptions(cliOpts, cliOptions, cliOptList)

	// Ensure a non-empty passphrase is set when asking for encryption
	if (opts.Encrypt || opts.Decrypt) && len(opts.CipherPassphrase) == 0 {
		// Fallback on the environment
		opts.CipherPassphrase = os.Getenv("PGBK_CIPHER_PASS")

		if len(opts.CipherPassphrase) == 0 {
			return fmt.Errorf("cannot use an empty passphrase for encryption")
		}
	}

	// When asked to decrypt the backups, do it here and exit, we have all
	// required input (passphrase and backup directory)
	if opts.Decrypt {
		// Avoid getting wrong globs from the config file since we are
		// using the remaining args from the command line that are
		// usually as a list of databases to dump
		globs := []string{}
		for _, v := range cliOptList {
			if v == "include-dbs" {
				globs = opts.Dbnames
				break
			}
		}

		if err := decryptDirectory(opts.Directory, opts.CipherPassphrase, opts.Jobs, globs); err != nil {
			return err
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
	pgDumpVersion := pgToolVersion("pg_dump")
	if pgDumpVersion < 80400 {
		return fmt.Errorf("provided pg_dump is older than 8.4, unable use it.")
	}

	if opts.Upload == "s3" && opts.S3Bucket == "" {
		return fmt.Errorf("a bucket is mandatory when upload is s3")
	}

	if opts.Upload == "gcs" && opts.GCSBucket == "" {
		return fmt.Errorf("a bucket is mandatory when upload is gcs")
	}

	if opts.Upload == "azure" && opts.AzureContainer == "" {
		return fmt.Errorf("a container is mandatory when upload is azure")
	}

	// Parse the connection information
	l.Verboseln("processing input connection parameters")
	conninfo, err := prepareConnInfo(opts.Host, opts.Port, opts.Username, opts.ConnDb)
	if err != nil {
		return fmt.Errorf("could not compute connection string: %w", err)
	}

	defer postBackupHook(opts.PostHook)
	if err := preBackupHook(opts.PreHook); err != nil {
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

	l.Infoln("dumping globals")
	if err := dumpGlobals(opts.Directory, opts.TimeFormat, conninfo, producedFiles); err != nil {
		return fmt.Errorf("pg_dumpall -g failed: %w", err)
	}

	db, err := dbOpen(conninfo)
	if err != nil {
		return fmt.Errorf("connection to PostgreSQL failed: %w", err)
	}
	defer db.Close()

	l.Infoln("dumping instance configuration")
	var verr *pgVersionError
	if err := dumpSettings(opts.Directory, opts.TimeFormat, db, producedFiles); err != nil {
		if errors.As(err, &verr) {
			l.Warnln(err)
		} else {
			return fmt.Errorf("could not dump configuration parameters: %w", err)
		}
	}

	if err := dumpConfigFiles(opts.Directory, opts.TimeFormat, db, producedFiles); err != nil {
		return fmt.Errorf("could not dump configuration files: %w", err)
	}

	databases, err := listDatabases(db, opts.WithTemplates, opts.ExcludeDbs, opts.Dbnames)
	if err != nil {
		return err
	}
	l.Verboseln("databases to dump:", databases)

	if err := pauseReplicationWithTimeout(db, opts.PauseTimeout); err != nil {
		return err
	}

	exitCode := 0
	maxWorkers := opts.Jobs
	numJobs := len(databases)
	jobs := make(chan *dump, numJobs)
	results := make(chan *dump, numJobs)

	// start workers - thanks gobyexample.com
	l.Verbosef("launching %d workers", maxWorkers)
	for w := 0; w < maxWorkers; w++ {
		go dumper(w, jobs, results, producedFiles)
	}

	defDbOpts := defaultDbOpts(opts)

	var passphrase string
	if opts.Encrypt {
		passphrase = opts.CipherPassphrase
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
			TimeFormat:       opts.TimeFormat,
			ConnString:       conninfo,
			CipherPassphrase: passphrase,
			EncryptKeepSrc:   opts.EncryptKeepSrc,
			ExitCode:         -1,
			PgDumpVersion:    pgDumpVersion,
		}

		l.Verbosef("sending dump job for database %s to worker pool", dbname)
		jobs <- d
	}

	canDumpACL := true
	canDumpConfig := true
	// collect the result of the jobs
	for j := 0; j < numJobs; j++ {
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

			b, err = dumpCreateDBAndACL(db, dbname, force)
			var verr *pgVersionError
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
			c, err = dumpDBConfig(db, dbname)
			if err != nil {
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

			aclpath := formatDumpPath(d.Directory, d.TimeFormat, "createdb.sql", dbname, d.When, 0)
			if err := os.MkdirAll(filepath.Dir(aclpath), 0755); err != nil {
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

			fmt.Fprintf(f, "%s", b)
			fmt.Fprintf(f, "%s", c)

			f.Close()

			// Have its checksum computed
			producedFiles <- sumFileJob{
				Path:    aclpath,
				SumAlgo: d.Options.SumAlgo,
			}

			l.Infoln("dump of ACL and configuration of", dbname, "to", aclpath, "done")
		}
	}

	if err := resumeReplication(db); err != nil {
		l.Errorln(err)
	}
	db.Close()

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

	var repo Repo

	switch opts.Upload {
	case "s3":
		repo, err = NewS3Repo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload to S3: %w", err)
		}
	case "sftp":
		repo, err = NewSFTPRepo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload over SFTP: %w", err)
		}
	case "gcs":
		repo, err = NewGCSRepo(opts)
		if err != nil {
			return fmt.Errorf("failed to prepare upload to GCS: %w", err)
		}
	case "azure":
		repo, err = NewAzRepo(opts)
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

		if err := purgeDumps(opts.Directory, dbname, o.PurgeKeep, limit); err != nil {
			retVal = err
		}

		if opts.PurgeRemote && repo != nil {
			if err := purgeRemoteDumps(repo, opts.Directory, dbname, o.PurgeKeep, limit); err != nil {
				retVal = err
			}
		}
	}

	for _, other := range []string{"pg_globals", "pg_settings", "hba_file", "ident_file"} {
		limit := now.Add(defDbOpts.PurgeInterval)
		if err := purgeDumps(opts.Directory, other, defDbOpts.PurgeKeep, limit); err != nil {
			retVal = err
		}

		if opts.PurgeRemote && repo != nil {
			if err := purgeRemoteDumps(repo, opts.Directory, other, defDbOpts.PurgeKeep, limit); err != nil {
				retVal = err
			}
		}
	}

	return
}

func defaultDbOpts(opts options) *dbOpts {
	dbo := dbOpts{
		Format:        opts.Format,
		Jobs:          opts.DirJobs,
		CompressLevel: opts.CompressLevel,
		SumAlgo:       opts.SumAlgo,
		PurgeInterval: opts.PurgeInterval,
		PurgeKeep:     opts.PurgeKeep,
		PgDumpOpts:    opts.PgDumpOpts,
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
	lock := formatDumpPath(d.Directory, d.TimeFormat, "lock", dbname, time.Time{}, 0)
	flock, locked, err := lockPath(lock)
	if err != nil {
		return fmt.Errorf("unable to lock %s: %s", lock, err)
	}

	if !locked {
		return fmt.Errorf("could not acquire lock for %s", dbname)
	}

	d.When = time.Now()

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

	file := formatDumpPath(d.Directory, d.TimeFormat, fileEnd, dbname, d.When, d.Options.CompressLevel)
	formatOpt := fmt.Sprintf("-F%c", d.Options.Format)

	command := execPath("pg_dump")
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
		for _, line := range strings.Split(string(stdoutStderr), "\n") {
			if line != "" {
				l.Errorf("[%s] %s\n", dbname, line)
			}
		}
		if err := unlockPath(flock); err != nil {
			l.Errorf("could not release lock for %s: %s", dbname, err)
			flock.Close()
		}
		return err
	}
	if len(stdoutStderr) > 0 {
		for _, line := range strings.Split(string(stdoutStderr), "\n") {
			if line != "" {
				l.Infof("[%s] %s\n", dbname, line)
			}
		}
	}

	if err := unlockPath(flock); err != nil {
		flock.Close()
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
	return nil
}

func dumper(id int, jobs <-chan *dump, results chan<- *dump, fc chan<- sumFileJob) {
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

func relPath(basedir, path string) string {
	target, err := filepath.Rel(basedir, path)
	if err != nil {
		l.Warnf("could not get relative path from %s: %s\n", path, err)
		target = path
	}

	prefix := fmt.Sprintf("..%c", os.PathSeparator)
	for strings.HasPrefix(target, prefix) {
		target = strings.TrimPrefix(target, prefix)
	}

	return target
}

func execPath(prog string) string {
	binFile := prog
	if runtime.GOOS == "windows" {
		binFile = fmt.Sprintf("%s.exe", prog)
	}

	if binDir != "" {
		return filepath.Join(binDir, binFile)
	}

	return binFile
}

func cleanDBName(dbname string) string {
	// We do not want a database name starting with a dot to avoid creating hidden files
	if strings.HasPrefix(dbname, ".") {
		dbname = "_" + dbname
	}

	// If there is a path separator in the database name, we do not want to
	// create the dump in a subdirectory or in a parent directory
	if strings.ContainsRune(dbname, os.PathSeparator) {
		dbname = strings.ReplaceAll(dbname, string(os.PathSeparator), "_")
	}

	// Always remove slashes to avoid issues with filenames on windows
	if strings.ContainsRune(dbname, '/') {
		dbname = strings.ReplaceAll(dbname, "/", "_")
	}

	return dbname
}

func formatDumpPath(dir string, timeFormat string, suffix string, dbname string, when time.Time, compressLevel int) string {
	var f, s, d string

	// Avoid attacks on the database name
	dbname = cleanDBName(dbname)

	d = dir
	if dbname != "" {
		d = strings.Replace(dir, "{dbname}", dbname, -1)
	}

	s = suffix
	if suffix == "" {
		s = "dump"
	}

	// Output is "dir(formatted)/dbname_date.suffix" when the
	// input time is not zero, otherwise do not include the date
	// and time. Reference time for time.Format(): "Mon Jan 2
	// 15:04:05 MST 2006"
	if when.IsZero() {
		f = fmt.Sprintf("%s.%s", dbname, s)
	} else {
		f = fmt.Sprintf("%s_%s.%s", dbname, when.Format(timeFormat), s)
	}

	if suffix == "sql" && compressLevel > 0 {
		f = f + ".gz"
	}

	return filepath.Join(d, f)
}

func pgToolVersion(tool string) int {
	vs, err := exec.Command(execPath(tool), "--version").Output()
	if err != nil {
		l.Warnf("failed to retrieve version of %s: %s", tool, err)
		return 0
	}

	var maj, min, rev, numver int
	n, _ := fmt.Sscanf(string(vs), tool+" (PostgreSQL) %d.%d.%d", &maj, &min, &rev)

	if n == 3 {
		// Before PostgreSQL 10, the format si MAJ.MIN.REV
		numver = (maj*100+min)*100 + rev
	} else if n == 2 {
		// From PostgreSQL 10, the format si MAJ.REV, so the rev ends
		// up in min with the scan
		numver = maj*10000 + min
	} else {
		// We have the special case of the development version, where the
		// format is MAJdevel
		fmt.Sscanf(string(vs), tool+" (PostgreSQL) %ddevel", &maj)
		numver = maj * 10000
	}

	l.Verboseln(tool, "version is:", numver)

	return numver
}

func dumpGlobals(dir string, timeFormat string, conninfo *ConnInfo, fc chan<- sumFileJob) error {
	command := execPath("pg_dumpall")
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

	if pgToolVersion("pg_dumpall") < 90300 {
		env = os.Environ()
		env = append(env, conninfo.MakeEnv()...)
	} else {
		args = append(args, "-d", conninfo.String())
	}

	file := formatDumpPath(dir, timeFormat, "sql", "pg_globals", time.Now(), 0)
	args = append(args, "-f", file)

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}

	pgDumpallCmd := exec.Command(command, args...)
	pgDumpallCmd.Env = env
	l.Verboseln("running:", pgDumpallCmd)
	stdoutStderr, err := pgDumpallCmd.CombinedOutput()
	if err != nil {
		for _, line := range strings.Split(string(stdoutStderr), "\n") {
			if line != "" {
				l.Errorln(line)
			}
		}
		return err
	}
	if len(stdoutStderr) > 0 {
		for _, line := range strings.Split(string(stdoutStderr), "\n") {
			if line != "" {
				l.Infoln(line)
			}
		}
	}

	if fc != nil {
		fc <- sumFileJob{
			Path: file,
		}
	}

	return nil
}

func dumpSettings(dir string, timeFormat string, db *pg, fc chan<- sumFileJob) error {

	file := formatDumpPath(dir, timeFormat, "out", "pg_settings", time.Now(), 0)

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}

	s, err := showSettings(db)
	if err != nil {
		return err
	}

	// Use a Buffer to avoid creating an empty file
	if len(s) > 0 {
		l.Verboseln("writing settings to:", file)
		if err := ioutil.WriteFile(file, []byte(s), 0644); err != nil {
			return err
		}

		if fc != nil {
			fc <- sumFileJob{
				Path: file,
			}
		}
	}

	return nil
}

func dumpConfigFiles(dir string, timeFormat string, db *pg, fc chan<- sumFileJob) error {
	for _, param := range []string{"hba_file", "ident_file"} {
		file := formatDumpPath(dir, timeFormat, "out", param, time.Now(), 0)

		if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
			return err
		}

		s, err := extractFileFromSettings(db, param)
		if err != nil {
			return err
		}

		// Use a Buffer to avoid creating an empty file
		if len(s) > 0 {
			l.Verbosef("writing contents of '%s' to: %s", param, file)
			if err := ioutil.WriteFile(file, []byte(s), 0644); err != nil {
				return err
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

func decryptDirectory(dir string, password string, workers int, globs []string) error {

	// Run a pool of workers to decrypt concurrently
	var wg sync.WaitGroup

	// Workers pick paths from the file queue
	fq := make(chan string)

	// We need a channel to know if a worker got an error at some point and
	// return an error
	ret := make(chan bool, workers)

	// Start workers that listen for filenames to decrypt until the queue
	// is closed
	for i := 0; i < workers; i++ {
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
				if err := decryptFile(file, password); err != nil {
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
		l.Warnln("no candidate file found for decryption. Maybe add a wildcard (*) to the patterns?")
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
	case _ = <-ret:
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

	Passphrase string
	KeepSrc    bool

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
func postProcessFiles(inFiles chan sumFileJob, wg *sync.WaitGroup, opts options) chan error {
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

	for i := 0; i < opts.Jobs; i++ {
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
					p, err := checksumFile(j.Path, j.SumAlgo)
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
							Path:       p,
							Passphrase: opts.CipherPassphrase,
							KeepSrc:    opts.EncryptKeepSrc,
							SumAlgo:    j.SumAlgo,
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
						Path:       j.Path,
						Passphrase: opts.CipherPassphrase,
						KeepSrc:    opts.EncryptKeepSrc,
						SumAlgo:    j.SumAlgo,
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

	for i := 0; i < opts.Jobs; i++ {
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
					encFiles, err := encryptFile(j.Path, j.Passphrase, j.KeepSrc)
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

	for i := 0; i < opts.Jobs; i++ {
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
					p, err := checksumFileList(j.Paths, j.SumAlgo, j.SumFile)
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

	var (
		repo Repo
		err  error
	)

	switch opts.Upload {
	case "s3":
		repo, err = NewS3Repo(opts)
		if err != nil {
			l.Errorln("failed to prepare upload to S3:", err)
			ret <- err
			repo = nil
		}
	case "sftp":
		repo, err = NewSFTPRepo(opts)
		if err != nil {
			l.Errorln("failed to prepare upload over SFTP:", err)
			ret <- err
			repo = nil
		}
	case "gcs":
		repo, err = NewGCSRepo(opts)
		if err != nil {
			l.Errorln("failed to prepare upload to GCS:", err)
			ret <- err
			repo = nil
		}
	case "azure":
		repo, err = NewAzRepo(opts)
		if err != nil {
			l.Errorln("failed to prepare upload to Azure", err)
			ret <- err
			repo = nil
		}
	}

	for i := 0; i < opts.Jobs; i++ {
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
					if err := repo.Upload(j.Path, relPath(opts.Directory, j.Path)); err != nil {
						l.Errorln(err)
						if !failed {
							ret <- err
							failed = true
						}
						continue
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
		for i := 0; i < opts.Jobs; i++ {
			<-done
		}
		close(encIn)

		for i := 0; i < opts.Jobs; i++ {
			<-done
		}
		close(sumEncIn)

		for i := 0; i < opts.Jobs; i++ {
			<-done
		}
		close(uploadIn)

		for i := 0; i < opts.Jobs; i++ {
			<-done
		}

		if repo != nil {
			repo.Close()
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
