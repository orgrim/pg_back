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
	"strings"
	"sync"
	"time"
)

var version = "2.0.1"
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
					l.Fatalln(err)
					os.Exit(1)
				}
			}
			os.Exit(0)
		} else {
			l.Fatalln(err)
			os.Exit(1)
		}
	}

	// Enable verbose mode or quiet mode as soon as possible
	l.SetVerbosity(cliOpts.Verbose, cliOpts.Quiet)

	// Load configuration file and allow the default configuration
	// file to be absent
	configOpts, err := loadConfigurationFile(cliOpts.CfgFile)
	if err != nil && cliOpts.CfgFile != defaultCfgFile {
		l.Fatalln(err)
		os.Exit(1)
	}

	// override options from the configuration file with ones from
	// the command line
	opts := mergeCliAndConfigOptions(cliOpts, configOpts, cliOptList)

	// When asked to decrypt the backups, do it here and exit, we have all
	// required input (passphrase and backup directory)
	if opts.Decrypt {
		if err := decryptDirectory(opts.Directory, opts.CipherPassphrase, opts.Jobs); err != nil {
			l.Fatalln(err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Remember when we start so that a purge interval of 0s won't remove the dumps we
	// are taking
	now := time.Now()

	if opts.BinDirectory != "" {
		binDir = opts.BinDirectory
	}

	// Ensure that pg_dump accepts the options we will give it
	pgDumpVersion := pgToolVersion("pg_dump")
	if pgDumpVersion < 80400 {
		l.Fatalln("provided pg_dump is older than 8.4, unable use it.")
		os.Exit(1)
	}

	// Parse the connection information
	l.Verboseln("processing input connection parameters")
	conninfo, err := prepareConnInfo(opts.Host, opts.Port, opts.Username, opts.ConnDb)
	if err != nil {
		l.Fatalln("could not compute connection string:", err)
		os.Exit(1)
	}

	if err := preBackupHook(opts.PreHook); err != nil {
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}

	// Use another goroutine to compute checksum of other files than
	// dumps. We just have to send the paths of files to it.
	producedFiles := make(chan string)

	// To stop gracefully, we would close the channel and tell the main
	// goroutine it's done
	ppDone := make(chan bool, 1)
	go func() {
		for {
			file, more := <-producedFiles
			if !more {
				break
			}

			if opts.SumAlgo != "none" {
				if err := checksumFile(file, opts.SumAlgo); err != nil {
					l.Warnln("checksum failed", err)
				}
			}

			if opts.Encrypt {
				encFiles, err := encryptFile(file, opts.CipherPassphrase, opts.EncryptKeepSrc)
				if err != nil {
					l.Warnln("encryption failed", err)
					continue
				}
				if opts.SumAlgo != "none" {
					for _, encFile := range encFiles {
						if err := checksumFile(encFile, opts.SumAlgo); err != nil {
							l.Warnln("checksum failed", err)
						}
					}
				}
			}
		}
		ppDone <- true
	}()

	l.Infoln("dumping globals")
	if err := dumpGlobals(opts.Directory, opts.TimeFormat, conninfo, producedFiles); err != nil {
		l.Fatalln("pg_dumpall -g failed:", err)
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}

	db, err := dbOpen(conninfo)
	if err != nil {
		l.Fatalln("connection to PostgreSQL failed:", err)
		postBackupHook(opts.PostHook)
		close(producedFiles)
		<-ppDone
		os.Exit(1)
	}
	defer db.Close()

	l.Infoln("dumping instance configuration")
	var verr *pgVersionError
	if err := dumpSettings(opts.Directory, opts.TimeFormat, db, producedFiles); err != nil {
		if errors.As(err, &verr) {
			l.Warnln(err)
		} else {
			db.Close()
			l.Fatalln("could not dump configuration parameters:", err)
			postBackupHook(opts.PostHook)
			close(producedFiles)
			<-ppDone
			os.Exit(1)
		}
	}

	if err := dumpConfigFiles(opts.Directory, opts.TimeFormat, db, producedFiles); err != nil {
		db.Close()
		l.Fatalln("could not dump configuration files:", err)
		postBackupHook(opts.PostHook)
		close(producedFiles)
		<-ppDone
		os.Exit(1)
	}

	databases, err := listDatabases(db, opts.WithTemplates, opts.ExcludeDbs, opts.Dbnames)
	if err != nil {
		l.Fatalln(err)
		db.Close()
		postBackupHook(opts.PostHook)
		close(producedFiles)
		<-ppDone
		os.Exit(1)
	}
	l.Verboseln("databases to dump:", databases)

	if err := pauseReplicationWithTimeout(db, opts.PauseTimeout); err != nil {
		db.Close()
		l.Fatalln(err)
		postBackupHook(opts.PostHook)
		close(producedFiles)
		<-ppDone
		os.Exit(1)
	}

	exitCode := 0
	maxWorkers := opts.Jobs
	numJobs := len(databases)
	jobs := make(chan *dump, numJobs)
	results := make(chan *dump, numJobs)

	// start workers - thanks gobyexample.com
	l.Verbosef("launching %d workers", maxWorkers)
	for w := 0; w < maxWorkers; w++ {
		go dumper(w, jobs, results)
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
			b, err = dumpCreateDBAndACL(db, dbname)
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

			aclpath := formatDumpPath(d.Directory, d.TimeFormat, "createdb.sql", dbname, d.When)
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
			producedFiles <- aclpath

			l.Infoln("dump of ACL and configuration of", dbname, "to", aclpath, "done")
		}
	}

	if err := resumeReplication(db); err != nil {
		l.Errorln(err)
	}
	db.Close()

	// purge old dumps per database and treat special files
	// (globals and settings) like databases
	l.Infoln("purging old dumps")

	for _, dbname := range databases {
		o, found := opts.PerDbOpts[dbname]
		if !found {
			o = defDbOpts
		}
		limit := now.Add(o.PurgeInterval)

		if err := purgeDumps(opts.Directory, dbname, o.PurgeKeep, limit); err != nil {
			exitCode = 1
		}
	}

	for _, other := range []string{"pg_globals", "pg_settings", "hba_file", "ident_file"} {
		limit := now.Add(defDbOpts.PurgeInterval)
		if err := purgeDumps(opts.Directory, other, defDbOpts.PurgeKeep, limit); err != nil {
			exitCode = 1
		}
	}

	postBackupHook(opts.PostHook)
	close(producedFiles)
	<-ppDone
	os.Exit(exitCode)
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

func (d *dump) dump() error {
	dbname := d.Database
	d.ExitCode = 1

	l.Infoln("dumping database", dbname)

	// Try to lock a file named after to database we are going to
	// dump to prevent stacking pg_back processes if pg_dump last
	// longer than a schedule of pg_back. If the lock cannot be
	// acquired, skip the dump and exit with an error.
	lock := formatDumpPath(d.Directory, d.TimeFormat, "lock", dbname, time.Time{})
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

	file := formatDumpPath(d.Directory, d.TimeFormat, fileEnd, dbname, d.When)
	formatOpt := fmt.Sprintf("-F%c", d.Options.Format)

	command := filepath.Join(binDir, "pg_dump")
	args := []string{formatOpt, "-f", file, "-w"}

	if fileEnd == "d" && d.Options.Jobs > 1 {
		if d.PgDumpVersion < 90300 {
			l.Warnln("provided pg_dump version does not support parallel jobs, ignoring option")
		} else {
			args = append(args, "-j", fmt.Sprintf("%d", d.Options.Jobs))
		}
	}

	// It is recommended to use --create with the plain format
	// from PostgreSQL 11 to get the ACL and configuration of the
	// database
	if d.PgDumpVersion >= 110000 && fileEnd == "sql" {
		args = append(args, "--create")
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

	// Add compression level option only if not dumping in the plain format
	if d.Options.CompressLevel >= 0 {
		if d.Options.Format != 'p' && d.Options.Format != 't' {
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

	d.Path = file

	// Compute the checksum tha goes with the dump file right
	// after the dump, to this is done concurrently too.
	if d.Options.SumAlgo != "none" {
		l.Infoln("computing checksum of", file)

		if err = checksumFile(file, d.Options.SumAlgo); err != nil {
			return fmt.Errorf("checksum failed: %s", err)
		}
	}

	// Encrypt the file
	if d.CipherPassphrase != "" {
		l.Infoln("encrypting", file)
		encFiles, err := encryptFile(file, d.CipherPassphrase, d.EncryptKeepSrc)
		if err != nil {
			return fmt.Errorf("encrypt failed: %s", err)

		}

		if d.Options.SumAlgo != "none" && len(encFiles) > 0 {
			if err := checksumFileList(encFiles, d.Options.SumAlgo, fmt.Sprintf("%s.age", file)); err != nil {
				return fmt.Errorf("failed to checksum encrypted files: %w", err)
			}
		}
	}

	d.ExitCode = 0
	return nil
}

func dumper(id int, jobs <-chan *dump, results chan<- *dump) {
	for j := range jobs {

		if err := j.dump(); err != nil {
			l.Errorln("dump of", j.Database, "failed:", err)
			results <- j
		} else {
			l.Infoln("dump of", j.Database, "to", j.Path, "done")
			results <- j
		}
	}
}

func formatDumpPath(dir string, timeFormat string, suffix string, dbname string, when time.Time) string {
	var f, s, d string

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

	return filepath.Join(d, f)
}

func pgToolVersion(tool string) int {
	vs, err := exec.Command(filepath.Join(binDir, tool), "--version").Output()
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

func dumpGlobals(dir string, timeFormat string, conninfo *ConnInfo, fc chan<- string) error {
	command := filepath.Join(binDir, "pg_dumpall")
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

	file := formatDumpPath(dir, timeFormat, "sql", "pg_globals", time.Now())
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
		fc <- file
	}

	return nil
}

func dumpSettings(dir string, timeFormat string, db *pg, fc chan<- string) error {

	file := formatDumpPath(dir, timeFormat, "out", "pg_settings", time.Now())

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
			fc <- file
		}
	}

	return nil
}

func dumpConfigFiles(dir string, timeFormat string, db *pg, fc chan<- string) error {
	for _, param := range []string{"hba_file", "ident_file"} {
		file := formatDumpPath(dir, timeFormat, "out", param, time.Now())

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
				fc <- file
			}
		}
	}
	return nil
}

func decryptDirectory(dir string, password string, workers int) error {

	// Run a pool of workers to decrypt concurrently
	var wg sync.WaitGroup

	// Workers pick paths from the file queue
	fq := make(chan string)

	// We need a channel to know if a worker got an error at some point and
	// return an error
	ret := make(chan bool, workers)

	// Start workers than listen for filenames to decrypt until the queue
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

	// Read the directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("unable to read directory %s: %w", dir, err)
	}

	for _, path := range entries {
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
					fq <- file
				}
			}
			continue
		}

		file := filepath.Join(dir, path.Name())
		if strings.HasSuffix(file, ".age") {
			fq <- file
		}
	}

	close(fq)
	wg.Wait()

	select {
	case _ = <-ret:
		return fmt.Errorf("failure in decrypt, please examine logs")
	default:
		return nil
	}
}
