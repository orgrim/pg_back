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
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/spf13/pflag"
	"gopkg.in/ini.v1"
)

func TestValidateDumpFormat(t *testing.T) {
	var tests = []string{"p", "plain", "c", "custom", "t", "tar", "d", "directory"}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			if err := validateDumpFormat(st); err != nil {
				t.Errorf("got %q, wnat nil", err)
			}
		})
	}

	tests = []string{"bad", "plaino", "pl", "dir"}
	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			if err := validateDumpFormat(st); err == nil {
				t.Errorf("got nil, wnat an error")
			}
		})
	}

}

func TestValidatePurgeKeepValue(t *testing.T) {
	var tests = []struct {
		give      string
		want      int
		wantError bool
	}{
		{"all", -1, false},
		{"18446744073709551615000", -1, true},
		{"50", 50, false},
		{"-10", -1, true},
	}

	l.logger.SetOutput(ioutil.Discard)
	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := validatePurgeKeepValue(st.give)
			if err == nil && st.wantError {
				t.Errorf("excepted an error got nil")
			} else if err != nil && !st.wantError {
				t.Errorf("did not want an error, got %s", err)
			}
			if got != st.want {
				t.Errorf("got %q, want %q", got, st.want)
			}
		})
	}
}

func TestValidatePurgeTimeLimitValue(t *testing.T) {
	var tests = []struct {
		give      string
		want      time.Duration
		wantError bool
	}{
		{"0", 0, false},
		{"5", -432000000000000, false}, // a literal number is time.Duration in ns
		{"18446744073709551615000", 0, true},
		{"-1h", 3600000000000, false},
		{"", 0, true},
		{"-1", 86400000000000, false}, // no unit means days, negative intervals are allowed
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := validatePurgeTimeLimitValue(st.give)
			if err == nil && st.wantError {
				t.Errorf("excepted an error got nil")
			} else if err != nil && !st.wantError {
				t.Errorf("did not want an error, got %s", err)
			}
			if got != st.want {
				t.Errorf("got %q, want %q", got, st.want)
			}
		})
	}
}

func TestValidateYesNoOption(t *testing.T) {
	var tests = []struct {
		give      string
		want      bool
		wantError bool
	}{
		{"y", true, false},
		{"Y", true, false},
		{"n", false, false},
		{"N", false, false},
		{"yes", true, false},
		{"Yes", true, false},
		{" no", false, false},
		{"NO", false, false},
		{"wrong", false, true},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := validateYesNoOption(st.give)
			if err == nil && st.wantError {
				t.Errorf("excepted an error got nil")
			} else if err != nil && !st.wantError {
				t.Errorf("did not want an error, got %s", err)
			}
			if got != st.want {
				t.Errorf("got %v, want %v", got, st.want)
			}
		})
	}
}

func TestValidateEnum(t *testing.T) {
	var tests = []struct {
		give      string
		allowed   []string
		wantError bool
	}{
		{"a", []string{"a", "b", "c"}, false},
		{"d", []string{"a", "b", "c"}, true},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := validateEnum(st.give, st.allowed)
			if got == nil && st.wantError {
				t.Errorf("excepted an error got nil")
			}

			if got != nil && !st.wantError {
				t.Errorf("did not expect an error, got %s", got)
			}
		})
	}
}

func TestDefaultOptions(t *testing.T) {
	timeFormat := time.RFC3339
	if runtime.GOOS == "windows" {
		timeFormat = "2006-01-02_15-04-05"
	}

	var want = options{
		Directory:     "/var/backups/postgresql",
		Format:        'c',
		DirJobs:       1,
		CompressLevel: -1,
		Jobs:          1,
		PauseTimeout:  3600,
		PurgeInterval: -30 * 24 * time.Hour,
		PurgeKeep:     0,
		SumAlgo:       "none",
		CfgFile:       "/etc/pg_back/pg_back.conf",
		TimeFormat:    timeFormat,
		Upload:        "none",
		AzureEndpoint: "blob.core.windows.net",
	}

	got := defaultOptions()

	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("DefaultOptions() mismatch (-want +got):\n%s", diff)
	}
}

func TestParseCli(t *testing.T) {
	timeFormat := time.RFC3339
	if runtime.GOOS == "windows" {
		timeFormat = "2006-01-02_15-04-05"
	}

	var (
		defaults = defaultOptions()
		tests    = []struct {
			args       []string
			want       options
			help       bool
			version    bool
			err        string
			legacyConf string
		}{
			{
				[]string{"-b", "test", "-Z", "2", "a", "b"},
				options{
					Directory:     "test",
					Dbnames:       []string{"a", "b"},
					Format:        'c',
					DirJobs:       1,
					CompressLevel: 2,
					Jobs:          1,
					PauseTimeout:  3600,
					PurgeInterval: -30 * 24 * time.Hour,
					PurgeKeep:     0,
					SumAlgo:       "none",
					CfgFile:       "/etc/pg_back/pg_back.conf",
					TimeFormat:    timeFormat,
					Upload:        "none",
					AzureEndpoint: "blob.core.windows.net",
				},
				false,
				false,
				"",
				"",
			},
			{
				[]string{"-t", "--without-templates"},
				options{
					Directory:     "/var/backups/postgresql",
					WithTemplates: false,
					Format:        'c',
					DirJobs:       1,
					CompressLevel: -1,
					Jobs:          1,
					PauseTimeout:  3600,
					PurgeInterval: -30 * 24 * time.Hour,
					PurgeKeep:     0,
					SumAlgo:       "none",
					CfgFile:       "/etc/pg_back/pg_back.conf",
					TimeFormat:    timeFormat,
					Upload:        "none",
					AzureEndpoint: "blob.core.windows.net",
				},
				false,
				false,
				"",
				"",
			},
			{
				[]string{"--help"},
				defaults,
				true,
				false,
				"",
				"",
			},
			{
				[]string{"--version"},
				defaults,
				false,
				true,
				"",
				"",
			},
			{
				[]string{"--convert-legacy-config", "some/path"},
				defaults,
				false,
				false,
				"",
				"some/path",
			},
			{
				[]string{"--upload", "wrong"},
				options{
					Directory:        "/var/backups/postgresql",
					Format:           'c',
					DirJobs:          1,
					CompressLevel:    -1,
					Jobs:             1,
					PauseTimeout:     3600,
					PurgeInterval:    -30 * 24 * time.Hour,
					PurgeKeep:        0,
					SumAlgo:          "none",
					CfgFile:          "/etc/pg_back/pg_back.conf",
					TimeFormat:       timeFormat,
					Encrypt:          true,
					CipherPassphrase: "testpass",
					Upload:           "wrong",
					AzureEndpoint:    "blob.core.windows.net",
				},
				false,
				false,
				"invalid value for --upload: value not found in [none s3 sftp gcs azure]",
				"",
			},
		}
	)

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			var (
				opts options
				err  error
			)

			// reset pflag default flagset between each sub test
			pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

			// when testing for help or version the usage is output to stderr, discard it with a pipe
			if st.help || st.version {
				oldStdout := os.Stdout
				oldStderr := os.Stderr
				_, w, _ := os.Pipe()
				os.Stderr = w
				os.Stdout = w
				opts, _, err = parseCli(st.args)
				os.Stderr = oldStderr
				os.Stdout = oldStdout
			} else {
				opts, _, err = parseCli(st.args)
			}

			var errVal *parseCliResult

			if err != nil {
				if errors.As(err, &errVal) {
					if errVal.ShowHelp != st.help {
						t.Errorf("got %v, want %v for help flag\n", errVal.ShowHelp, st.help)
					}
					if errVal.ShowVersion != st.version {
						t.Errorf("got %v, want %v for version flag\n", errVal.ShowVersion, st.version)
					}
					if errVal.LegacyConfig != st.legacyConf {
						t.Errorf("got %v, want %v for convert legacy config flag\n", errVal.LegacyConfig, st.legacyConf)
					}
				} else if st.err != err.Error() {
					t.Errorf("got error %v, expected %v", st.err, err)
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
	timeFormat := time.RFC3339
	if runtime.GOOS == "windows" {
		timeFormat = "2006-01-02_15-04-05"
	}

	var tests = []struct {
		params []string
		fail   bool
		want   options
	}{
		{
			[]string{"backup_directory = test", "port = 5433"},
			false,
			options{
				Directory:     "test",
				Port:          5433,
				Format:        'c',
				DirJobs:       1,
				CompressLevel: -1,
				Jobs:          1,
				PauseTimeout:  3600,
				PurgeInterval: -30 * 24 * time.Hour,
				PurgeKeep:     0,
				SumAlgo:       "none",
				CfgFile:       "/etc/pg_back/pg_back.conf",
				TimeFormat:    timeFormat,
				Upload:        "none",
				AzureEndpoint: "blob.core.windows.net",
			},
		},
		{ // ensure comma separated lists work
			[]string{"backup_directory = test", "include_dbs = a, b, postgres", "compress_level = 9"},
			false,
			options{
				Directory:     "test",
				Dbnames:       []string{"a", "b", "postgres"},
				Format:        'c',
				DirJobs:       1,
				CompressLevel: 9,
				Jobs:          1,
				PauseTimeout:  3600,
				PurgeInterval: -30 * 24 * time.Hour,
				PurgeKeep:     0,
				SumAlgo:       "none",
				CfgFile:       "/etc/pg_back/pg_back.conf",
				TimeFormat:    timeFormat,
				Upload:        "none",
				AzureEndpoint: "blob.core.windows.net",
			},
		},
		{
			[]string{"timestamp_format = rfc3339"},
			false,
			options{
				Directory:     "/var/backups/postgresql",
				Format:        'c',
				DirJobs:       1,
				CompressLevel: -1,
				Jobs:          1,
				PauseTimeout:  3600,
				PurgeInterval: -30 * 24 * time.Hour,
				PurgeKeep:     0,
				SumAlgo:       "none",
				CfgFile:       "/etc/pg_back/pg_back.conf",
				TimeFormat:    timeFormat,
				Upload:        "none",
				AzureEndpoint: "blob.core.windows.net",
			},
		},
		{
			[]string{"timestamp_format = legacy"},
			false,
			options{
				Directory:     "/var/backups/postgresql",
				Format:        'c',
				DirJobs:       1,
				CompressLevel: -1,
				Jobs:          1,
				PauseTimeout:  3600,
				PurgeInterval: -30 * 24 * time.Hour,
				PurgeKeep:     0,
				SumAlgo:       "none",
				CfgFile:       "/etc/pg_back/pg_back.conf",
				TimeFormat:    "2006-01-02_15-04-05",
				Upload:        "none",
				AzureEndpoint: "blob.core.windows.net",
			},
		},
		{
			[]string{"timestamp_format = wrong"},
			true,
			defaultOptions(),
		},
		{ // with an error output is the default
			[]string{},
			true,
			defaultOptions(),
		},
		{
			[]string{
				"backup_directory = test",
				"pg_dump_options = -O -x",
				"[db]",
				"purge_older_than = 15",
				"parallel_backup_jobs = 2",
				"with_blobs = true",
				"compress_level = 2",
			},
			false,
			options{
				Directory:     "test",
				Format:        'c',
				DirJobs:       1,
				CompressLevel: -1,
				Jobs:          1,
				PauseTimeout:  3600,
				PurgeInterval: -30 * 24 * time.Hour,
				PurgeKeep:     0,
				SumAlgo:       "none",
				CfgFile:       "/etc/pg_back/pg_back.conf",
				TimeFormat:    timeFormat,
				PgDumpOpts:    []string{"-O", "-x"},
				PerDbOpts: map[string]*dbOpts{"db": &dbOpts{
					Format:        'c',
					SumAlgo:       "none",
					Jobs:          2,
					CompressLevel: 2,
					PurgeInterval: -15 * 24 * time.Hour,
					PurgeKeep:     0,
					PgDumpOpts:    []string{"-O", "-x"},
					WithBlobs:     1,
				}},
				Upload:        "none",
				AzureEndpoint: "blob.core.windows.net",
			},
		},
		{
			[]string{
				"backup_directory = test",
				"pg_dump_options = -O -x",
				"compress_level = 3",
				"[db]",
				"purge_older_than = 15",
				"parallel_backup_jobs = 2",
				"pg_dump_options =",
				"with_blobs = false",
			},
			false,
			options{
				Directory:     "test",
				Format:        'c',
				DirJobs:       1,
				CompressLevel: 3,
				Jobs:          1,
				PauseTimeout:  3600,
				PurgeInterval: -30 * 24 * time.Hour,
				PurgeKeep:     0,
				SumAlgo:       "none",
				CfgFile:       "/etc/pg_back/pg_back.conf",
				TimeFormat:    timeFormat,
				PgDumpOpts:    []string{"-O", "-x"},
				PerDbOpts: map[string]*dbOpts{"db": &dbOpts{
					Format:        'c',
					SumAlgo:       "none",
					CompressLevel: 3,
					Jobs:          2,
					PurgeInterval: -15 * 24 * time.Hour,
					PurgeKeep:     0,
					PgDumpOpts:    []string{},
					WithBlobs:     2,
				}},
				Upload:        "none",
				AzureEndpoint: "blob.core.windows.net",
			},
		},
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

			var got options
			got, err = loadConfigurationFile(f.Name())
			if err != nil && !st.fail {
				t.Errorf("expected an error")
			}
			if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("loadConfigurationFile() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMergeCliAndConfigoptions(t *testing.T) {
	timeFormat := time.RFC3339
	if runtime.GOOS == "windows" {
		timeFormat = "2006-01-02_15-04-05"
	}

	want := options{
		BinDirectory:  "/bin",
		Directory:     "test",
		Host:          "localhost",
		Port:          5433,
		Username:      "test",
		ConnDb:        "postgres",
		ExcludeDbs:    []string{"a", "b"},
		Dbnames:       []string{"b", "c", "d"},
		WithTemplates: true,
		Format:        'd',
		DirJobs:       2,
		CompressLevel: 4,
		Jobs:          4,
		PauseTimeout:  60,
		PurgeInterval: -7 * 24 * time.Hour,
		PurgeKeep:     5,
		SumAlgo:       "sha256",
		PreHook:       "touch /tmp/pre-hook",
		PostHook:      "touch /tmp/post-hook",
		CfgFile:       "/etc/pg_back/pg_back.conf",
		TimeFormat:    timeFormat,
		Upload:        "none",
		AzureEndpoint: "blob.core.windows.net",
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
		"compress",
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

	got := mergeCliAndConfigOptions(want, defaultOptions(), cliOptList)
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("mergeCliAndConfigOptions() mismatch (-want +got):\n%s", diff)
	}
}

func TestError(t *testing.T) {
	err := &parseCliResult{}

	s := fmt.Sprintf("%s", err)
	if s != "please exit now" {
		t.Errorf("func (*parseCliResult) Error() failed")
	}
}

func TestValidateConfigurationFile(t *testing.T) {
	var tests = []struct {
		input string
		fails bool
		err   string
	}{
		{"bin_directory = /usr/bin\nbackup_directory = /backups\n", false, ""},
		{"wrong = fails\n", true, "unknown parameter in configuration file: wrong"},
		{"bin_directory = /usr/bin\n[b1]\nwith_blobs = true\n\n[b2]\nwrong = fails\n", true, "unknown parameter in configuration file for db b2: wrong"},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			cfg, err := ini.Load([]byte(st.input))
			if err != nil {
				t.Errorf("failed to load input: %q", st.input)
			}

			got := validateConfigurationFile(cfg)
			if got == nil && st.fails {
				t.Errorf("wanted an error got <nil>")
			}

			if got != nil {
				if got.Error() != st.err {
					t.Errorf("got error %q, want %q", got, st.err)
				}
			}
		})
	}
}
