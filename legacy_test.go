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
	"bytes"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestReadLegacyConf(t *testing.T) {
	var tests = []struct {
		conf string
		want []string
	}{
		{"# comment\n" +
			"var=thing\n",
			[]string{},
		},
		{"# comment\n" +
			"# Example: PGBK_OPTS=(\"-Fc\" \"-T\" \"tmp*\")\n" +
			"PGBK_OPTS=(\"-Fc\")\n",
			[]string{
				"PGBK_OPTS=(\"-Fc\")",
			},
		},
		{"# comment\n" +
			"PGBK_PURGE=30 # 30 days\n" +
			"#PGBK_HOSTNAME=\n" +
			"PGBK_PORT=5433\n",
			[]string{
				"PGBK_PURGE=30 # 30 days",
				"PGBK_PORT=5433",
			},
		},
		{" PGBK_PURGE=30\n" +
			"SIGNATURE_ALGO=\"sha256\"\n",
			[]string{
				"PGBK_PURGE=30",
				"SIGNATURE_ALGO=\"sha256\"",
			},
		},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			f := bytes.NewBufferString(st.conf)

			got, err := readLegacyConf(f)
			if err != nil {
				t.Errorf("got an error: %s", err)
			} else {
				if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("got %v, want %v", got, st.want)
				}
			}
		})
	}
}

func TestStripEndComment(t *testing.T) {
	var tests = []struct {
		line string
		want string
	}{
		{"value", "value"},
		{"value#comment", "value"},
		{"value #comment", "value"},
		{"value\\\\ #comment", "value\\\\"},

		{"'value' # comment", "'value'"},
		{"'value \\'quoted\\'' # comment", "'value \\'quoted\\''"},
		{"'value \\'quoted # com'me#nt", "'value \\'quoted # com'me"},
		{"'value \"\\\"\\'quoted #\" com'me#nt", "'value \"\\\"\\'quoted #\" com'me"},

		{"\"value\" # comment", "\"value\""},
		{"\"value \\\"quoted\\\"\" # comment", "\"value \\\"quoted\\\"\""},
		{"\"value \\\"quoted # com\"me#nt", "\"value \\\"quoted # com\"me"},
		{"\"value \\'\\\"quoted #' com\"me#nt", "\"value \\'\\\"quoted #' com\"me"},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := stripEndComment(st.line)
			if got != st.want {
				t.Errorf("got %v, want %v", got, st.want)
			}
		})
	}
}

func TestConvertLegacyConf(t *testing.T) {
	var tests = []struct {
		input []string
		want  string
	}{
		{[]string{
			"PGBK_BIN=",
			"PGBK_BACKUP_DIR=/var/backups/postgresql",
			"PGBK_TIMESTAMP='%Y-%m-%d_%H-%M-%S'",
			"PGBK_PURGE=30",
			"PGBK_PURGE_MIN_KEEP=0",
			"PGBK_OPTS=(\"-F\" \"c\")",
			"PGBK_DBLIST=\"db1 db2\"",
			"PGBK_EXCLUDE=\"sampledb1 testdb2\"",
			"PGBK_WITH_TEMPLATES=\"no\"",
			"PGBK_STANDBY_PAUSE_TIMEOUT=3600",
			"PGBK_HOSTNAME=/tmp",
			"PGBK_PORT=5432",
			"PGBK_USERNAME=",
			"PGBK_CONNDB=postgres",
			"PGBK_PRE_BACKUP_COMMAND=/bin/true",
			"PGBK_POST_BACKUP_COMMAND=/bin/false",
		}, "bin_directory = \n" +
			"backup_directory = /var/backups/postgresql\n" +
			"timestamp_format = legacy\n" +
			"purge_older_than = 30\n" +
			"purge_min_keep = 0\n" +
			"format = custom\n" +
			"pg_dump_options = \n" +
			"include_dbs = db1, db2\n" +
			"exclude_dbs = sampledb1, testdb2\n" +
			"with_templates = false\n" +
			"pause_timeout = 3600\n" +
			"host = /tmp\n" +
			"port = 5432\n" +
			"user = \n" +
			"dbname = postgres\n" +
			"pre_backup_hook = /bin/true\n" +
			"post_backup_hook = /bin/false\n"},
		{[]string{
			"PGBK_TIMESTAMP=\"%Y-%m-%d\"",
			"PGBK_OPTS=(\"--format=c\" \"-T\" \"tmp*\")",
			"PGBK_WITH_TEMPLATES=\"yes\"",
		}, "timestamp_format = rfc3339\n" +
			"format = custom\n" +
			"pg_dump_options = -T tmp*\n" +
			"with_templates = true\n"},
		{[]string{
			"PGBK_OPTS=(\"--format\" \"c\" \"-T\" \"tmp spaced\")",
		}, "format = custom\n" +
			"pg_dump_options = -T \"tmp spaced\"\n"},
		{[]string{
			"PGBK_OPTS=\"-Fplain --create\"",
		}, "format = plain\n" +
			"pg_dump_options = --create\n"},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := convertLegacyConf(st.input)
			if got != st.want {
				t.Errorf("got %v, want %v", got, st.want)
			}
		})
	}
}
