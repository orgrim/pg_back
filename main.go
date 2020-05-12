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
	"fmt"
	"github.com/spf13/pflag"
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
	file := FormatDumpPath(d.Directory, "dump", dbname, d.When)

	command := "pg_dump"
	args := []string{"-Fc", "-f", file}

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

func (d *Dump) Checksum() error {
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

type CliOptions struct {
	directory     string
	host          string
	port          int
	username      string
	connDb        string
	excludeDbs    []string
	dbnames       []string
	withTemplates bool
	jobs          int
	pauseTimeout  int
	purgeInterval string
	purgeKeep     string
	sumAlgo       string
}

func ParseCli() CliOptions {
	opts := CliOptions{}

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "pg_goback dumps some PostgreSQL databases\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  pg_goback [OPTION]... [DBNAME]...\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		pflag.CommandLine.SortFlags = false
		pflag.PrintDefaults()
	}

	pflag.StringVarP(&opts.directory, "backup-directory", "b", "/var/backups/postgresql", "store dump files there")
	pflag.StringSliceVarP(&opts.excludeDbs, "exclude-dbs", "D", []string{}, "list of databases to exclude")
	pflag.BoolVarP(&opts.withTemplates, "with-templates", "t", false, "include templates")
	pflag.IntVarP(&opts.pauseTimeout, "pause-timeout", "T", 3600, "abort if replication cannot be paused after this number of seconds")
	pflag.IntVarP(&opts.jobs, "jobs", "j", 1, "dump this many databases concurrently")
	pflag.StringVarP(&opts.sumAlgo, "checksum-algo", "S", "none", "signature algorithm: none sha1 sha224 sha256 sha384 sha512")
	pflag.StringVarP(&opts.purgeInterval, "purge-older-than", "P", "30", "purge backups older than this duration in days\nuse an interval with units \"s\" (seconds), \"m\" (minutes) or \"h\" (hours)\nfor less than a day.")
	pflag.StringVarP(&opts.purgeKeep, "purge-min-keep", "K", "0", "minimum number of dumps to keep when purging or 'all' to keep everything\n")
	pflag.StringVarP(&opts.host, "host", "h", "", "database server host or socket directory")
	pflag.IntVarP(&opts.port, "port", "p", 0, "database server port number")
	pflag.StringVarP(&opts.username, "username", "U", "", "connect as specified database user")
	pflag.StringVarP(&opts.connDb, "dbname", "d", "", "connect to database name")

	helpF := pflag.BoolP("help", "?", false, "print usage")
	versionF := pflag.BoolP("version", "V", false, "print version")

	pflag.Parse()

	if *helpF {
		pflag.Usage()
		os.Exit(0)
	}

	if *versionF {
		fmt.Printf("pg_goback version %v\n", version)
		os.Exit(0)
	}

	opts.dbnames = pflag.Args()
	return opts
}

func main() {
	var (
		databases []string
		limit     time.Time
	)

	CliOpts := ParseCli()

	// validate purge options and do the extra parsing
	keep := PurgeValidateKeepValue(CliOpts.purgeKeep)

	if interval, err := PurgeValidateTimeLimitValue(CliOpts.purgeInterval); err != nil {
		l.Fatalln(err)
	} else {
		// computing the limit before taking the dumps ensure
		// a purge interval of 0s won't remove the dumps we
		// are taking
		limit = time.Now().Add(interval)
	}

	l.Infoln("Dumping globals")
	if err := DumpGlobals(CliOpts.directory, CliOpts.host,
		CliOpts.port, CliOpts.username, CliOpts.connDb); err != nil {
		l.Fatalln("pg_dumpall -g failed")
	}

	conninfo := PrepareConnInfo(CliOpts.host, CliOpts.port, CliOpts.username, CliOpts.connDb)

	db, ok := DbOpen(conninfo)
	if !ok {
		os.Exit(1)
	}
	defer db.Close()

	l.Infoln("Dumping instance configuration")
	if err := DumpSettings(CliOpts.directory, db); err != nil {
		db.Close()
		l.Fatalln("Could not dump configuration parameters")
	}

	if len(CliOpts.dbnames) > 0 {
		databases = CliOpts.dbnames
	} else {
		var ok bool

		databases, ok = ListAllDatabases(db, CliOpts.withTemplates)
		if !ok {
			db.Close()
			os.Exit(0)
		}

		// exclure les bases
		if len(CliOpts.excludeDbs) > 0 {
			filtered := []string{}
			for _, d := range databases {
				found := false
				for _, e := range CliOpts.excludeDbs {
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

	if err := PauseReplicationWithTimeout(db, CliOpts.pauseTimeout); err != nil {
		db.Close()
		l.Fatalln(err)
	}

	exitCode := 0
	maxWorkers := CliOpts.jobs
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
			Directory: CliOpts.directory,
			Host:      CliOpts.host,
			Port:      CliOpts.port,
			Username:  CliOpts.username,
			SumAlgo:   CliOpts.sumAlgo,
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
		if err := PurgeDumps(CliOpts.directory, dbname, keep, limit); err != nil {
			exitCode = 1
		}
	}

	for _, other := range []string{"pg_globals", "pg_settings"} {
		if err := PurgeDumps(CliOpts.directory, other, keep, limit); err != nil {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}
