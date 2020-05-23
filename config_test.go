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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/spf13/pflag"
	"io/ioutil"
	"os"
	"testing"
)

func TestValidateDumpFormat(t *testing.T) {
	var tests = []string{"pl", "plain", "c", "custom", "t", "tar", "d", "dir", "directory"}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			if err := ValidateDumpFormat(st); err != nil {
				t.Errorf("got %q, wnat nil", err)
			}
		})
	}

	tests = []string{"bad", "plaino"}
	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			if err := ValidateDumpFormat(st); err == nil {
				t.Errorf("got nil, wnat an error")
			}
		})
	}

}

func TestDefaultOptions(t *testing.T) {
	var want = Options{
		Directory:     "/var/backups/postgresql",
		Format:        "custom",
		DirJobs:       1,
		Jobs:          1,
		PauseTimeout:  3600,
		PurgeInterval: "30",
		PurgeKeep:     "0",
		SumAlgo:       "none",
		CfgFile:       "/etc/pg_goback/pg_goback.conf",
	}

	got := DefaultOptions()

	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("DefaultOptions() mismatch (-want +got):\n%s", diff)
	}
}

func TestParseCli(t *testing.T) {
	var (
		defaults = DefaultOptions()
		tests    = []struct {
			args    []string
			want    Options
			help    bool
			version bool
		}{
			{[]string{"-b", "test", "a", "b"}, Options{Directory: "test", Dbnames: []string{"a", "b"}, Format: "custom", DirJobs: 1, Jobs: 1, PauseTimeout: 3600, PurgeInterval: "30", PurgeKeep: "0", SumAlgo: "none", CfgFile: "/etc/pg_goback/pg_goback.conf"}, false, false},
			{[]string{"-t", "--without-templates"}, Options{Directory: "/var/backups/postgresql", WithTemplates: false, Format: "custom", DirJobs: 1, Jobs: 1, PauseTimeout: 3600, PurgeInterval: "30", PurgeKeep: "0", SumAlgo: "none", CfgFile: "/etc/pg_goback/pg_goback.conf"}, false, false},
			{[]string{"--help"}, defaults, true, false},
			{[]string{"--version"}, defaults, false, true},
		}
	)

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			// reset pflag default flagset between each sub test
			pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
			// set our test command line arguments
			configParseCliInput = st.args

			var (
				opts Options
				err  error
			)
			// when testing for help or version the usage is output to stderr, discard it with a pipe
			if st.help || st.version {
				oldStdout := os.Stdout
				oldStderr := os.Stderr
				_, w, _ := os.Pipe()
				os.Stderr = w
				os.Stdout = w
				opts, _, err = ParseCli()
				os.Stderr = oldStderr
				os.Stdout = oldStdout
			} else {
				opts, _, err = ParseCli()
			}

			var errVal *ParseCliError

			if err != nil && errors.As(err, &errVal) {
				if errVal.ShowHelp != st.help {
					t.Errorf("got %v, want %v for help flag\n", errVal.ShowHelp, st.help)
				}
				if errVal.ShowVersion != st.version {
					t.Errorf("got %v, want %v for version flag\n", errVal.ShowVersion, st.version)
				}
			} else {
				if diff := cmp.Diff(st.want, opts, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("ParseCli() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestLoadConfigurationFile(t *testing.T) {
	var tests = []struct {
		params []string
		fail   bool
		want   Options
	}{
		{[]string{"backup_directory = test", "port = 5433"}, false, Options{Directory: "test", Port: 5433, Format: "custom", DirJobs: 1, Jobs: 1, PauseTimeout: 3600, PurgeInterval: "30", PurgeKeep: "0", SumAlgo: "none", CfgFile: "/etc/pg_goback/pg_goback.conf"}},
		{[]string{"backup_directory = test", "include_dbs = a, b, postgres"}, false, Options{Directory: "test", Dbnames: []string{"a", "b", "postgres"}, Format: "custom", DirJobs: 1, Jobs: 1, PauseTimeout: 3600, PurgeInterval: "30", PurgeKeep: "0", SumAlgo: "none", CfgFile: "/etc/pg_goback/pg_goback.conf"}}, // ensure comma separated lists work
		{[]string{}, true, DefaultOptions()}, // with an error output is the default
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {

			// create a temporary file to feed the function
			f, err := ioutil.TempFile("", "test")
			if err != nil {
				t.Errorf("could not setup test: %v\n", err)
			}
			for _, l := range st.params {
				fmt.Fprintf(f, "%s\n", l)
			}
			f.Close()

			if st.fail {
				os.Remove(f.Name())
			} else {
				defer os.Remove(f.Name())
			}

			var got Options
			got, err = LoadConfigurationFile(f.Name())
			if err != nil && !st.fail {
				t.Errorf("expected an error")
			}
			if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("LoadConfigurationFile() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMergeCliAndConfigOptions(t *testing.T) {
	want := Options{
		BinDirectory:  "/bin",
		Directory:     "test",
		Host:          "localhost",
		Port:          5433,
		Username:      "test",
		ConnDb:        "postgres",
		ExcludeDbs:    []string{"a", "b"},
		Dbnames:       []string{"b", "c", "d"},
		WithTemplates: true,
		Format:        "dir",
		DirJobs:       2,
		Jobs:          4,
		PauseTimeout:  60,
		PurgeInterval: "7",
		PurgeKeep:     "5",
		SumAlgo:       "sha256",
		PreHook:       "touch /tmp/pre-hook",
		PostHook:      "touch /tmp/post-hook",
		CfgFile:       "/etc/pg_goback/pg_goback.conf",
	}

	cliOptList := []string{
		"bin-directory",
		"backup-directory",
		"exclude-dbs",
		"include-dbs",
		"with-templates",
		"pause-timeout",
		"jobs",
		"format",
		"parallel-backup-jobs",
		"checksum-algo",
		"purge-older-than",
		"purge-min-keep",
		"pre-backup-hook",
		"post-backup-hook",
		"host",
		"port",
		"username",
		"dbname",
	}

	got := MergeCliAndConfigOptions(want, DefaultOptions(), cliOptList)
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("MergeCliAndConfigOptions() mismatch (-want +got):\n%s", diff)
	}
}
