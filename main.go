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

	l.Infoln("Dumping database", dbname)

	// Try to lock a file named after to database we are going to
	// dump to prevent stacking pg_back processes if pg_dump last
	// longer than a schedule of pg_back. If the lock cannot be
	// acquired, skip the dump and exit with an error.
	f, locked, lerr := LockPath(FormatDumpPath(d.Directory, "lock", dbname, time.Time{}))
	if lerr != nil {
		return lerr
	}

	if !locked {
		l.Errorln("Could not acquire lock for ", dbname)
		return fmt.Errorf("lock error")
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

	file := FormatDumpPath(d.Directory, fileEnd, dbname, d.When)
	formatOpt := fmt.Sprintf("-F%c", []rune(d.Format)[0])

	command := "pg_dump"
	args := []string{formatOpt, "-f", file}

	AppendConnectionOptions(&args, d.Host, d.Port, d.Username)
	args = append(args, dbname)

	pgDumpCmd := exec.Command(command, args...)
	stdoutStderr, err := pgDumpCmd.CombinedOutput()
	if err != nil {
		l.Errorln(string(stdoutStderr))
		l.Errorln(err)
		if lerr = UnLockPath(f); lerr != nil {
			f.Close()
		}
		return err
	}
	if len(stdoutStderr) > 0 {
		l.Infof("%s\n", stdoutStderr)
	}

	if lerr = UnLockPath(f); lerr != nil {
		f.Close()
		return lerr
	}

	d.Path = file

	// Compute the checksum tha goes with the dump file right
	// after the dump, to this is done concurrently too.
	if d.SumAlgo != "none" {
		l.Infoln("Computing checksum of", file)

		if err = ChecksumFile(file, d.SumAlgo); err != nil {
			return err
		}
	}

	d.ExitCode = 0
	return nil
}

func dumper(id int, jobs <-chan *Dump, results chan<- *Dump) {
	for j := range jobs {

		if err := j.Dump(); err != nil {
			l.Errorln("Dump of", j.Database, "failed")
			results <- j
		} else {
			l.Infoln("Dump of", j.Database, "to", j.Path, "done")
			results <- j
		}
	}
}

func AppendConnectionOptions(args *[]string, host string, port int, username string) {
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

func FormatDumpPath(dir string, suffix string, dbname string, when time.Time) string {
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

func DumpGlobals(dir string, host string, port int, username string, connDb string) error {
	command := "pg_dumpall"
	args := []string{"-g"}

	AppendConnectionOptions(&args, host, port, username)
	if connDb != "" {
		args = append(args, "-l", connDb)
	}

	file := FormatDumpPath(dir, "sql", "pg_globals", time.Now())
	args = append(args, "-f", file)

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		l.Errorln(err)
		return err
	}

	pgDumpallCmd := exec.Command(command, args...)
	stdoutStderr, err := pgDumpallCmd.CombinedOutput()
	if err != nil {
		l.Errorf("%s\n", stdoutStderr)
		l.Errorln(err)
		return err
	}
	if len(stdoutStderr) > 0 {
		l.Infof("%s\n", stdoutStderr)
	}
	return nil
}

func DumpSettings(dir string, db *DB) error {

	file := FormatDumpPath(dir, "out", "pg_settings", time.Now())

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		l.Errorln(err)
		return err
	}

	f, err := os.Create(file)
	if err != nil {
		l.Errorln(err)
		return err
	}

	n, err := ShowSettings(f, db)

	// Do not leave an empty file
	f.Close()
	if err != nil {
		os.Remove(file)
		return err
	}

	if n == 0 {
		os.Remove(file)
	}

	return nil
}

func main() {
	var (
		databases []string
		limit     time.Time
	)

	// Parse commanline arguments first so that we can quit if you
	// have shown usage or version string and load a non default
	// configuration file
	CliOpts, cliOptList, perr := ParseCli()
	var pce *ParseCliError
	if perr != nil && errors.As(perr, &pce) {
		os.Exit(0)
	}

	// Load configuration file and allow the default configuration file to be absent
	ConfigOpts, err := LoadConfigurationFile(CliOpts.CfgFile)
	if err != nil && CliOpts.CfgFile != defaultCfgFile {
		l.Fatalln(err)
		os.Exit(1)
	}

	Opts := MergeCliAndConfigOptions(CliOpts, ConfigOpts, cliOptList)

	// validate purge options and do the extra parsing
	keep := PurgeValidateKeepValue(Opts.PurgeKeep)

	if interval, err := PurgeValidateTimeLimitValue(Opts.PurgeInterval); err != nil {
		l.Fatalln(err)
		os.Exit(1)
	} else {
		// computing the limit before taking the dumps ensure
		// a purge interval of 0s won't remove the dumps we
		// are taking
		limit = time.Now().Add(interval)
	}

	if err := PreBackupHook(Opts.PreHook); err != nil {
		PostBackupHook(Opts.PostHook)
		os.Exit(1)
	}

	l.Infoln("Dumping globals")
	if err := DumpGlobals(Opts.Directory, Opts.Host,
		Opts.Port, Opts.Username, Opts.ConnDb); err != nil {
		l.Fatalln("pg_dumpall -g failed")
		PostBackupHook(Opts.PostHook)
		os.Exit(1)
	}

	conninfo := PrepareConnInfo(Opts.Host, Opts.Port, Opts.Username, Opts.ConnDb)

	db, ok := DbOpen(conninfo)
	if !ok {
		PostBackupHook(Opts.PostHook)
		os.Exit(1)
	}
	defer db.Close()

	l.Infoln("Dumping instance configuration")
	if err := DumpSettings(Opts.Directory, db); err != nil {
		db.Close()
		l.Fatalln("Could not dump configuration parameters")
		PostBackupHook(Opts.PostHook)
		os.Exit(1)
	}

	if len(Opts.Dbnames) > 0 {
		databases = Opts.Dbnames
	} else {
		var ok bool

		databases, ok = ListAllDatabases(db, Opts.WithTemplates)
		if !ok {
			db.Close()
			PostBackupHook(Opts.PostHook)
			os.Exit(0)
		}

		// exclure les bases
		if len(Opts.ExcludeDbs) > 0 {
			filtered := []string{}
			for _, d := range databases {
				found := false
				for _, e := range Opts.ExcludeDbs {
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

	if err := PauseReplicationWithTimeout(db, Opts.PauseTimeout); err != nil {
		db.Close()
		l.Fatalln(err)
		PostBackupHook(Opts.PostHook)
		os.Exit(1)
	}

	exitCode := 0
	maxWorkers := Opts.Jobs
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
			Directory: Opts.Directory,
			Format:    Opts.Format,
			Host:      Opts.Host,
			Port:      Opts.Port,
			Username:  Opts.Username,
			SumAlgo:   Opts.SumAlgo,
			ExitCode:  -1,
		}

		jobs <- d
	}

	// collect the result of the jobs
	for j := 0; j < numJobs; j++ {
		d := <-results
		if d.ExitCode > 0 {
			exitCode = 1
		} else if d.ExitCode == 0 {
			// When it is ok, dump the creation and ACL commands as SQL commands
			if db.version >= 110000 || db.version < 90000 {
				continue
			}
			dbname := d.Database
			aclpath := FormatDumpPath(d.Directory, "sql", dbname, d.When)
			if err := os.MkdirAll(filepath.Dir(aclpath), 0755); err != nil {
				l.Errorln(err)
				exitCode = 1
			} else {
				if f, err := os.Create(aclpath); err != nil {
					l.Errorln(err)
					exitCode = 1
				} else {
					l.Infoln("Dumping database creation and ACL commands of database", dbname)

					n, err := DumpCreateDBAndACL(f, db, dbname)
					if err != nil {
						l.Errorln("Dump of ACL failed")
						exitCode = 1
					}
					f.Close()
					if n == 0 {
						l.Infoln("No ACL found for", dbname)
						os.Remove(aclpath)
					} else {
						l.Infoln("Dump of ACL of", dbname, "to", aclpath, "done")
					}
				}
			}
		}
	}

	if err := ResumeReplication(db); err != nil {
		l.Errorln(err)
	}
	db.Close()

	// purge
	l.Infoln("Purging old dumps")

	for _, dbname := range databases {
		if err := PurgeDumps(Opts.Directory, dbname, keep, limit); err != nil {
			exitCode = 1
		}
	}

	for _, other := range []string{"pg_globals", "pg_settings"} {
		if err := PurgeDumps(Opts.Directory, other, keep, limit); err != nil {
			exitCode = 1
		}
	}

	PostBackupHook(Opts.PostHook)
	os.Exit(exitCode)
}
