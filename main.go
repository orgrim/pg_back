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
	"strings"
	"time"
	"path/filepath"
	"log"
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
}

func (d *Dump) Dump() error {
	dbname := d.Database
	file := FormatDumpPath(d.Directory, "dump", dbname)

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		l.err.Println("error:", err)
		return err
	}
	command := "pg_dump"
	args := []string{"-Fc", "-f", file}
	
	AppendConnectionOptions(&args, d.Host, d.Port, d.Username)
	args = append(args, dbname)
	
	pgDumpCmd := exec.Command(command, args...)
	stdoutStderr, err := pgDumpCmd.CombinedOutput()
	if err != nil {
		l.err.Println("error:", string(stdoutStderr))
		l.err.Println("error:", err)
		return err
	}
	if len(stdoutStderr) > 0 {
		l.out.Printf("%s\n", stdoutStderr)
	}

	d.Path = file
	
	return err
}

func (d *Dump) Checksum() error {
	return nil
}

func dumper(id int, jobs <-chan *Dump, results chan<- int) {
	for j := range jobs {
		l.out.Println("[", id, "] Dumping", j.Database)
		if err := j.Dump(); err != nil {
			l.err.Println("[", id, "] Dump of", j.Database, "failed")
			results <- 1
		} else {
			l.out.Println("[", id, "] Dump of", j.Database, "to", j.Path, "done")
			results <- 0
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

func FormatTimeNow() string {
	// Reference time for time.Format(): "Mon Jan 2 15:04:05 MST 2006"
	now := time.Now()
	return now.Format("2006-01-02_15-04-05")
}

func FormatDumpPath(dir string, suffix string, dbname string) string {
	var f, s, d string

	d = dir
	if dbname != "" {
		d = strings.Replace(dir, "{dbname}", dbname, -1)
	}

	s = suffix
	if suffix == "" {
		s = "dump"
	}

	// dir(formatted)/dbname_date.suffix
	f = fmt.Sprintf("%s_%s.%s", dbname, FormatTimeNow(), s)

	return filepath.Join(d, f)
}

func DumpGlobals(dir string, host string, port int, username string, connDb string) error {
	command := "pg_dumpall"
	args := []string{"-g"}
	
	AppendConnectionOptions(&args, host, port, username)
	if connDb != "" {
		args = append(args, "-l", connDb)
	}

	file := FormatDumpPath(dir, "sql", "pg_globals")
	args = append(args, "-f", file)

	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		log.Println("error:", err)
		return err
	}
	
	pgDumpallCmd := exec.Command(command, args...)
	stdoutStderr, err := pgDumpallCmd.CombinedOutput()
	if err != nil {
		fmt.Println("error:", string(stdoutStderr))
		fmt.Println("error:", err)
		return err
	}
	if len(stdoutStderr) > 0 {
		fmt.Printf("%s\n", stdoutStderr)
	}
	return nil
}
/*

struct dump pour stocker ce que l'user veut en fonction de la conf


=> fonction pgdump pour créer et exécuter la commande pg_dump
=> fonction pg_dumpall -g avec dumpacl



*/

type CliOptions struct {
	directory     string
	host          string
	port          int
	username      string
	connDb        string
	dbnames       []string
	withTemplates bool
	jobs          int
}

func ParseCli() CliOptions {
	opts := CliOptions{}

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "pg_goback dumps some PostgreSQL database\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  pg_goback [OPTION]... [DBNAME]...\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		pflag.CommandLine.SortFlags = false
		pflag.PrintDefaults()
	}

	pflag.StringVarP(&opts.directory, "backup-directory", "b", "/var/backups/postgresql", "store dump files there")
	pflag.BoolVarP(&opts.withTemplates, "with-templates", "t", false, "include templates\n")
	pflag.IntVarP(&opts.jobs, "jobs", "j", 1, "use this many parallel jobs to dump\n")
	pflag.StringVarP(&opts.host, "host", "h", "", "database server host or socket directory")
	pflag.IntVarP(&opts.port, "port", "p", 0, "database server port number")
	pflag.StringVarP(&opts.username, "username", "U", "", "connect as specified database user")
	pflag.StringVarP(&opts.connDb, "dbname", "d", "", "connect to database name")

	helpF := pflag.BoolP("help", "?", false, "print usage")
	versionF := pflag.BoolP("version", "v", false, "print version")

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

type mylog struct {
	err *log.Logger
	out *log.Logger
}

func SetupLogger() mylog {
	e := log.New(os.Stderr, "ERROR: ", log.LstdFlags)
	o := log.New(os.Stdout, "INFO: ", log.LstdFlags)
	return mylog{
		err: e,
		out: o,
	}
}

var l = SetupLogger()

func main() {
	var databases []string

	CliOpts := ParseCli()

	// mettre en pause la replication

	// pg_dumpall -g
	err := DumpGlobals(CliOpts.directory, CliOpts.host, CliOpts.port, CliOpts.username, CliOpts.connDb)
	if err != nil {
		l.err.Fatalln("pg_dumpall -g failed")
	}

	if len(CliOpts.dbnames) > 0 {
		databases = CliOpts.dbnames
	} else {
		var ok bool

		conninfo := PrepareConnInfo(CliOpts.host, CliOpts.port, CliOpts.username, CliOpts.connDb)

		db, ok := DbOpen(conninfo)
		if !ok {
			os.Exit(1)
		}

		databases, ok = ListAllDatabases(db, CliOpts.withTemplates)
		if !ok {
			db.Close()
			os.Exit(0)
		}
		db.Close()
		// exclure les bases
	}

	exitCode := 0
	maxWorkers := CliOpts.jobs
	numJobs := len(databases)
	jobs := make(chan *Dump, numJobs)
	results := make(chan int, numJobs)

	// start workers - thanks gobyexample.com
	for w := 0; w < maxWorkers; w++ {
		go dumper(w, jobs, results)
	}

	// feed the database
	for _, dbname := range databases {
		d := &Dump{
			Database: dbname,
			Directory: CliOpts.directory,
			Host: CliOpts.host,
			Port: CliOpts.port,
			Username: CliOpts.username,
		}
		jobs <- d
	}

	// collect the return codes of the jobs
	for j := 0; j < numJobs; j++ {
		rc := <-results
		if rc > 0 {
			exitCode = 1
		}
	}

	os.Exit(exitCode)
}
