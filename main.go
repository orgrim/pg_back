// pg_goback
//
// Copyright 2020 Nicolas Thauvin. All rights reserved.
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

type Dump struct {
	// Database is ne name of the database to dump
	Database string

	// Path is the output file or directory of the dump
	// a directory is output with the directory format of pg_dump
	// It remains empty until after the dump is done
	Path string

	// Directory is the target directory where to create the dump
	Directory string

	// Format of the dump
	Format string

	// Number of parallel jobs for directory format
	Jobs int

	// Connection parameters
	Host     string
	Port     int
	Username string

	// Checksum
	SumAlgo string

	// Result
	When     time.Time
	ExitCode int
}

func (d *Dump) Dump() error {
	dbname := d.Database
	d.ExitCode = 1

	l.Infoln("dumping database", dbname)

	// Try to lock a file named after to database we are going to
	// dump to prevent stacking pg_back processes if pg_dump last
	// longer than a schedule of pg_back. If the lock cannot be
	// acquired, skip the dump and exit with an error.
	lock := formatDumpPath(d.Directory, "lock", dbname, time.Time{})
	flock, locked, err := lockPath(lock)
	if err != nil {
		return fmt.Errorf("unable to lock %s: %s", lock, err)
	}

	if !locked {
		return fmt.Errorf("could not acquire lock for %s", dbname)
	}

	d.When = time.Now()

	var fileEnd string
	switch string([]rune(d.Format)[0]) {
	case "p":
		fileEnd = "sql"
	case "c":
		fileEnd = "dump"
	case "t":
		fileEnd = "tar"
	case "d":
		fileEnd = "d"
	}

	file := formatDumpPath(d.Directory, fileEnd, dbname, d.When)
	formatOpt := fmt.Sprintf("-F%c", []rune(d.Format)[0])

	command := filepath.Join(binDir, "pg_dump")
	args := []string{formatOpt, "-f", file}

	if fileEnd == "d" && d.Jobs > 0 {
		args = append(args, "-j", fmt.Sprintf("%d", d.Jobs))
	}

	appendConnectionOptions(&args, d.Host, d.Port, d.Username)
	args = append(args, dbname)

	pgDumpCmd := exec.Command(command, args...)
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
	if d.SumAlgo != "none" && fileEnd != "d" {
		l.Infoln("computing checksum of", file)

		if err = checksumFile(file, d.SumAlgo); err != nil {
			return fmt.Errorf("checksum failed: %s", err)
		}
	}

	d.ExitCode = 0
	return nil
}

func dumper(id int, jobs <-chan *Dump, results chan<- *Dump) {
	for j := range jobs {

		if err := j.Dump(); err != nil {
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

func formatDumpPath(dir string, suffix string, dbname string, when time.Time) string {
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
		f = fmt.Sprintf("%s_%s.%s", dbname, when.Format("2006-01-02_15-04-05"), s)
	}

	return filepath.Join(d, f)
}

func dumpGlobals(dir string, host string, port int, username string, connDb string) error {
	command := filepath.Join(binDir, "pg_dumpall")
	args := []string{"-g"}

	appendConnectionOptions(&args, host, port, username)
	if connDb != "" {
		args = append(args, "-l", connDb)
	}

	file := formatDumpPath(dir, "sql", "pg_globals", time.Now())
	args = append(args, "-f", file)

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}

	pgDumpallCmd := exec.Command(command, args...)
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

func dumpSettings(dir string, db *pg) error {

	file := formatDumpPath(dir, "out", "pg_settings", time.Now())

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}

	s, err := showSettings(db)
	if err != nil {
		return err
	}

	// Use a Buffer to avoid creating an empty file
	if len(s) > 0 {
		if err := ioutil.WriteFile(file, []byte(s), 0644); err != nil {
			return err
		}
	}

	return nil
}

var binDir string

func main() {
	var (
		databases []string
		limit     time.Time
	)

	// Parse commanline arguments first so that we can quit if you
	// have shown usage or version string and load a non default
	// configuration file
	cliOpts, cliOptList, perr := parseCli()
	var pce *ParseCliError
	if perr != nil && errors.As(perr, &pce) {
		os.Exit(0)
	}

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

	// validate purge options and do the extra parsing
	keep := validatePurgeKeepValue(opts.PurgeKeep)

	if interval, err := validatePurgeTimeLimitValue(opts.PurgeInterval); err != nil {
		l.Fatalln(err)
		os.Exit(1)
	} else {
		// computing the limit before taking the dumps ensure
		// a purge interval of 0s won't remove the dumps we
		// are taking
		limit = time.Now().Add(interval)
	}

	if opts.BinDirectory != "" {
		binDir = opts.BinDirectory
	}

	if err := preBackupHook(opts.PreHook); err != nil {
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}

	l.Infoln("dumping globals")
	if err := dumpGlobals(opts.Directory, opts.Host,
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
	if err := dumpSettings(opts.Directory, db); err != nil {
		db.Close()
		l.Fatalln("could not dump configuration parameters:", err)
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}

	if len(opts.Dbnames) > 0 {
		databases = opts.Dbnames
	} else {
		databases, err = listAllDatabases(db, opts.WithTemplates)
		if err != nil {
			l.Fatalln(err)
			db.Close()
			postBackupHook(opts.PostHook)
			os.Exit(0)
		}

		// exclure les bases
		if len(opts.ExcludeDbs) > 0 {
			filtered := []string{}
			for _, d := range databases {
				found := false
				for _, e := range opts.ExcludeDbs {
					if d == e {
						found = true
						break
					}
				}
				if !found {
					filtered = append(filtered, d)
				}
			}
			databases = filtered
		}
	}

	if err := pauseReplicationWithTimeout(db, opts.PauseTimeout); err != nil {
		db.Close()
		l.Fatalln(err)
		postBackupHook(opts.PostHook)
		os.Exit(1)
	}

	exitCode := 0
	maxWorkers := opts.Jobs
	numJobs := len(databases)
	jobs := make(chan *Dump, numJobs)
	results := make(chan *Dump, numJobs)

	// start workers - thanks gobyexample.com
	for w := 0; w < maxWorkers; w++ {
		go dumper(w, jobs, results)
	}

	// feed the database
	for _, dbname := range databases {
		d := &Dump{
			Database:  dbname,
			Directory: opts.Directory,
			Format:    opts.Format,
			Jobs:      opts.DirJobs,
			Host:      opts.Host,
			Port:      opts.Port,
			Username:  opts.Username,
			SumAlgo:   opts.SumAlgo,
			ExitCode:  -1,
		}

		jobs <- d
	}

	// collect the result of the jobs
	for j := 0; j < numJobs; j++ {
		d := <-results
		if d.ExitCode > 0 {
			exitCode = 1
		}

		// XXX use the custom error from dumpCreateDBAndACL()
		if db.version >= 110000 || db.version < 90000 {
			continue
		}

		dbname := d.Database

		l.Infoln("dumping database creation and ACL commands of", dbname)
		b, err := dumpCreateDBAndACL(db, dbname)
		if err != nil {
			l.Errorln(err)
			exitCode = 1
		}

		l.Infoln("dumping database configuration commands of", dbname)
		c, err := dumpDBConfig(db, dbname)
		if err != nil {
			l.Errorln(err)
			exitCode = 1
		}

		if len(b) > 0 || len(c) > 0 {

			aclpath := formatDumpPath(d.Directory, "sql", dbname, d.When)
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

			l.Infoln("dump of ACL of", dbname, "to", aclpath, "done")
		} else {
			l.Infoln("no ACL found for", dbname)
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
		if err := purgeDumps(opts.Directory, dbname, keep, limit); err != nil {
			exitCode = 1
		}
	}

	for _, other := range []string{"pg_globals", "pg_settings"} {
		if err := purgeDumps(opts.Directory, other, keep, limit); err != nil {
			exitCode = 1
		}
	}

	postBackupHook(opts.PostHook)
	os.Exit(exitCode)
}
