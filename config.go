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
	"strings"
)

type Options struct {
	directory     string
	host          string
	port          int
	username      string
	connDb        string
	excludeDbs    []string
	dbnames       []string
	withTemplates bool
	format        string
	dirJobs       int
	jobs          int
	pauseTimeout  int
	purgeInterval string
	purgeKeep     string
	sumAlgo       string
	preHook       string
	postHook      string
}

func ValidateDumpFormat(s string) error {
	for _, v := range []string{"plain", "custom", "tar", "directory"} {
		if strings.HasPrefix(v, s) {
			return nil
		}
	}
	return fmt.Errorf("invalid dump format %q", s)
}

func ParseCli() Options {
	opts := Options{}

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
	pflag.StringVarP(&opts.format, "format", "F", "custom", "database dump format: plain, custom, tar or directory")

	pflag.StringVarP(&opts.sumAlgo, "checksum-algo", "S", "none", "signature algorithm: none sha1 sha224 sha256 sha384 sha512")
	pflag.StringVarP(&opts.purgeInterval, "purge-older-than", "P", "30", "purge backups older than this duration in days\nuse an interval with units \"s\" (seconds), \"m\" (minutes) or \"h\" (hours)\nfor less than a day.")
	pflag.StringVarP(&opts.purgeKeep, "purge-min-keep", "K", "0", "minimum number of dumps to keep when purging or 'all' to keep everything\n")
	pflag.StringVar(&opts.preHook, "pre-backup-hook", "", "command to run before taking dumps")
	pflag.StringVar(&opts.postHook, "post-backup-hook", "", "command to run after taking dumps")
	pflag.StringVarP(&opts.host, "host", "h", "", "database server host or socket directory")
	pflag.IntVarP(&opts.port, "port", "p", 0, "database server port number")
	pflag.StringVarP(&opts.username, "username", "U", "", "connect as specified database user")
	pflag.StringVarP(&opts.connDb, "dbname", "d", "", "connect to database name")

	helpF := pflag.BoolP("help", "?", false, "print usage")
	versionF := pflag.BoolP("version", "V", false, "print version")

	pflag.Parse()

	changed := make([]string, 0)
	pflag.Visit(func(f *pflag.Flag) {
		changed = append(changed, f.Name)
	})

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
