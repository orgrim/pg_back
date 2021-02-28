// pg_goback
//
// Copyright 2020-2021 Nicolas Thauvin. All rights reserved.
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
	"time"
)

var version = "0.0.1"

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
	Host       string
	Port       int
	Username   string
	ConnString string

	// Result
	When     time.Time
	ExitCode int
}

type dbOpts struct {
	// Format of the dump
	Format string

	// Algorithm of the checksum of the file, "none" is used to
	// disable checksuming
	SumAlgo string

	// Number of parallel jobs for directory format
	Jobs int

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
	switch string([]rune(d.Options.Format)[0]) {
	case "p":
		fileEnd = "sql"
	case "c":
		fileEnd = "dump"
	case "t":
		fileEnd = "tar"
	case "d":
		fileEnd = "d"
	}

	file := formatDumpPath(d.Directory, d.TimeFormat, fileEnd, dbname, d.When)
	formatOpt := fmt.Sprintf("-F%c", []rune(d.Options.Format)[0])

	command := filepath.Join(binDir, "pg_dump")
	args := []string{formatOpt, "-f", file}

	if fileEnd == "d" && d.Options.Jobs > 1 {
		args = append(args, "-j", fmt.Sprintf("%d", d.Options.Jobs))
	}

	// It is recommended to use --create with the plain format
	// from PostgreSQL 11 to get the ACL and configuration of the
	// database
	if pgDumpVersion() >= 110000 && fileEnd == "sql" {
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
		args = append(args, "-B")
	}

	if len(d.Options.PgDumpOpts) > 0 {
		args = append(args, d.Options.PgDumpOpts...)
	}

	// Connection option a passed in as options or a connstring
	if len(d.ConnString) > 0 {
		if infos, err := parseConnInfo(d.ConnString); err != nil {
			return fmt.Errorf("cannot parse connstring: %w", err)
		} else {
			infos["dbname"] = dbname
			args = append(args, "-d", makeConnInfo(infos))
		}
	} else {
		appendConnectionOptions(&args, d.Host, d.Port, d.Username)
		// Finally the name of the database
		args = append(args, dbname)
	}

	pgDumpCmd := exec.Command(command, args...)
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

func appendConnectionOptions(args *[]string, host string, port int, username string) {
	if host != "" {
		*args = append(*args, "-h", host)
	}
	if port != 0 {
		*args = append(*args, "-p", fmt.Sprintf("%v", port))
	}
	if username != "" {
		*args = append(*args, "-U", username)
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

func pgDumpVersion() int {
	vs, err := exec.Command(filepath.Join(binDir, "pg_dump"), "-V").Output()
	if err != nil {
		l.Warnln("failed to retrieve version of pg_dump:", err)
		return 0
	}

	var maj, min, rev int
	fmt.Sscanf(string(vs), "pg_dump (PostgreSQL) %d.%d.%d", &maj, &min, &rev)

	return (maj*100+min)*100 + rev
}

func dumpGlobals(dir string, timeFormat string, host string, port int, username string, connDb string) error {
	command := filepath.Join(binDir, "pg_dumpall")
	args := []string{"-g"}

	// When connDb is a connstring use it instead of other
	// options. pg_dumpall does the same
	if strings.Contains(connDb, "=") {
		args = append(args, "-d", connDb)

		// pg_dumpall only connects to another database if it is given
		// with the -l option
		infos, err := parseConnInfo(connDb)
		if err != nil {
			return err
		}
		if dbname, ok := infos["dbname"]; ok {
			args = append(args, "-l", dbname)
		}
	} else {
		appendConnectionOptions(&args, host, port, username)
		if connDb != "" {
			args = append(args, "-l", connDb)
		}
	}

	file := formatDumpPath(dir, timeFormat, "sql", "pg_globals", time.Now())
	args = append(args, "-f", file)

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}

	pgDumpallCmd := exec.Command(command, args...)
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
	return nil
}

func dumpSettings(dir string, timeFormat string, db *pg) error {

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
	}

	return nil
}

func defaultDbOpts(opts options) *dbOpts {
	dbo := dbOpts{
		Format:        opts.Format,
		Jobs:          opts.DirJobs,
		SumAlgo:       opts.SumAlgo,
		PurgeInterval: opts.PurgeInterval,
		PurgeKeep:     opts.PurgeKeep,
	}
	return &dbo
}

var binDir string

func main() {
	// Parse commanline arguments first so that we can quit if we
	// have shown usage or version string. We may have to load a
	// non default configuration file
	cliOpts, cliOptList, perr := parseCli()
	var pce *parseCliResult
	if perr != nil {
		if errors.As(perr, &pce) {
			os.Exit(0)
		} else {
			l.Fatalln(perr)
			os.Exit(1)
		}
	}

	// Enable verbose mode as soon as possible
	l.SetVerbose(cliOpts.Verbose)

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

	// Remember when we start so that a purge interval of 0s won't remove the dumps we
	// are taking
	now := time.Now()

	if opts.BinDirectory != "" {
		binDir = opts.BinDirectory
	}

	if err := preBackupHook(opts.PreHook); err != nil {
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}

	l.Infoln("dumping globals")
	if err := dumpGlobals(opts.Directory, opts.TimeFormat, opts.Host,
		opts.Port, opts.Username, opts.ConnDb); err != nil {
		l.Fatalln("pg_dumpall -g failed:", err)
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}

	conninfo := prepareConnInfo(opts.Host, opts.Port, opts.Username, opts.ConnDb)

	db, err := dbOpen(conninfo)
	if err != nil {
		l.Fatalln("connection to PostgreSQL failed:", err)
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}
	defer db.Close()

	l.Infoln("dumping instance configuration")
	var verr *pgVersionError
	if err := dumpSettings(opts.Directory, opts.TimeFormat, db); err != nil {
		if errors.As(err, &verr) {
			l.Warnln(err)
		} else {
			db.Close()
			l.Fatalln("could not dump configuration parameters:", err)
			postBackupHook(opts.PostHook)
			os.Exit(1)
		}
	}

	databases, err := listDatabases(db, opts.WithTemplates, opts.ExcludeDbs, opts.Dbnames)
	if err != nil {
		l.Fatalln(err)
		db.Close()
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}
	l.Verboseln("databases to dump:", databases)

	if err := pauseReplicationWithTimeout(db, opts.PauseTimeout); err != nil {
		db.Close()
		l.Fatalln(err)
		postBackupHook(opts.PostHook)
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
	// feed the database
	for _, dbname := range databases {
		o, found := opts.PerDbOpts[dbname]
		if !found {
			o = defDbOpts
		}
		d := &dump{
			Database:   dbname,
			Options:    o,
			Directory:  opts.Directory,
			TimeFormat: opts.TimeFormat,
			Host:       opts.Host,
			Port:       opts.Port,
			Username:   opts.Username,
			ExitCode:   -1,
		}

		if strings.Contains(opts.ConnDb, "=") {
			d.ConnString = opts.ConnDb
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

	for _, other := range []string{"pg_globals", "pg_settings"} {
		limit := now.Add(defDbOpts.PurgeInterval)
		if err := purgeDumps(opts.Directory, other, defDbOpts.PurgeKeep, limit); err != nil {
			exitCode = 1
		}
	}

	postBackupHook(opts.PostHook)
	os.Exit(exitCode)
}
