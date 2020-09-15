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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"os"
	"regexp"
	"strings"
	"testing"
)

var (
	testdb *pg
)

func checkIfWeTest(t *testing.T) {
	if os.Getenv("PGBK_TEST_CONNINFO") == "" {
		t.Skip("testing with PostgreSQL disabled")
	}

	if testdb == nil {
		var err error
		testdb, err = dbOpen(os.Getenv("PGBK_TEST_CONNINFO"))
		if err != nil {
			t.Fatalf("expected an ok on dbOpen(), got %s", err)
		}
	}
}

func TestPrepareConnInfo(t *testing.T) {
	var tests = []struct {
		host     string
		port     int
		username string
		dbname   string
		want     string
	}{
		{"/tmp", 0, "", "", "host=/tmp application_name=pg_goback"},
		{"localhost", 5432, "postgres", "postgres", "host=localhost port=5432 user=postgres dbname=postgres application_name=pg_goback"},
		{"localhost", 0, "postgres", "postgres", "host=localhost user=postgres dbname=postgres application_name=pg_goback"},
		{"localhost", 5432, "", "postgres", "host=localhost port=5432 dbname=postgres application_name=pg_goback"},
		{"localhost", 5432, "postgres", "", "host=localhost port=5432 user=postgres application_name=pg_goback"},
		{"localhost", 0, "postgres", "", "host=localhost user=postgres application_name=pg_goback"},
	}

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			res := prepareConnInfo(subt.host, subt.port, subt.username, subt.dbname)
			if res != subt.want {
				t.Errorf("got '%s', want '%s'", res, subt.want)
			}
		})
	}
}

func TestSqlQuoteLiteral(t *testing.T) {
	var tests = []struct {
		input string
		want  string
	}{
		{"", "''"},
		{"'", "''''"},
		{"'; select 1 --", "'''; select 1 --'"},
		{"\\", "E'\\\\'"},
		{"'\\n", "E'''\\\\n'"},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := sqlQuoteLiteral(st.input)
			if got != st.want {
				t.Errorf("got '%s', want '%s'", got, st.want)
			}
		})
	}
}

func TestSqlQuoteIdent(t *testing.T) {
	var tests = []struct {
		input string
		want  string
	}{
		{"\"", "\"\""},
		{"", ""},
		{"\"; select 1 --", "\"\"; select 1 --"},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := sqlQuoteIdent(st.input)
			if got != st.want {
				t.Errorf("got '%s', want '%s'", got, st.want)
			}
		})
	}
}

func TestMakeACLCommands(t *testing.T) {
	var tests = []struct {
		input string
		want  string
	}{
		{"", ""},
		{"invalid", ""},
		{"=", ""},
		{"/", ""},
		{"=c/postgres", "REVOKE ALL ON DATABASE \"testdb\" FROM PUBLIC;\nSET SESSION AUTHORIZATION \"postgres\";\nGRANT CONNECT ON DATABASE \"testdb\" TO \"PUBLIC\";\nRESET SESSION AUTHORIZATION;\n"},
		{"=Tc/postgres", ""},
		{"testrole=CTc/testrole", ""},
		{"testrole=Cc/testrole", "REVOKE ALL ON DATABASE \"testdb\" FROM \"testrole\";\nGRANT CREATE ON DATABASE \"testdb\" TO \"testrole\";\nGRANT CONNECT ON DATABASE \"testdb\" TO \"testrole\";\n"},
		{"other=CT*c/testrole", "GRANT CREATE ON DATABASE \"testdb\" TO \"other\";\nGRANT TEMPORARY ON DATABASE \"testdb\" TO \"other\" WITH GRANT OPTION;\nGRANT CONNECT ON DATABASE \"testdb\" TO \"other\";\n"},
		{"other=T*/testrole", "GRANT TEMPORARY ON DATABASE \"testdb\" TO \"other\" WITH GRANT OPTION;\n"},
	}

	dbname := "testdb"
	owner := "testrole"

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := makeACLCommands(st.input, dbname, owner)
			if got != st.want {
				t.Errorf("got '%s', want '%s'", got, st.want)
			}
		})
	}
}

func TestDbOpen(t *testing.T) {
	if os.Getenv("PGBK_TEST_CONNINFO") == "" {
		t.Skip("testing with PostgreSQL disabled")
	}

	conninfo := os.Getenv("PGBK_TEST_CONNINFO")
	db, err := dbOpen(conninfo)
	if err != nil {
		t.Fatalf("expected an ok on dbOpen(), got %s", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("expected an okon db.Close(), got %s", err)
	}

	testdb, err = dbOpen(conninfo)
	if err != nil {
		t.Fatalf("expected an ok on dbOpen(), got %s", err)
	}
}

func TestListAllDatabases(t *testing.T) {
	var tests = []struct {
		templates bool
		want      []string
	}{
		{false, []string{"b1", "b2", "postgres"}},
		{true, []string{"b1", "b2", "postgres", "template1"}},
	}

	checkIfWeTest(t)

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := listAllDatabases(testdb, st.templates)
			if err != nil {
				t.Errorf("expected non nil error, got %q", err)
			}

			// sort result before comparing because we do not use order by in the queries
			if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty(), cmpopts.SortSlices(func(x, y string) bool { return x < y })); diff != "" {
				t.Errorf("listAllDatabases() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDumpDBConfig(t *testing.T) {
	var tests = []struct {
		want string
	}{
		{"ALTER ROLE \"u1\" IN DATABASE \"b1\" SET \"work_mem\" TO '1MB';\nALTER DATABASE \"b1\" SET \"log_min_duration_statement\" TO '10s';\nALTER DATABASE \"b1\" SET \"work_mem\" TO '5MB';\n"},
	}

	checkIfWeTest(t)

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := dumpDBConfig(testdb, "b1")
			if err != nil {
				t.Errorf("expected non nil error, got %q", err)
			}

			if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("dumpDBConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestShowSettings(t *testing.T) {
	checkIfWeTest(t)

	got, err := showSettings(testdb)
	if err != nil {
		t.Errorf("expected non nil error, got %q", err)
	}
	// we cannot exactly test the content, it depends on the version of PostgreSQL
	if got == "" {
		t.Errorf("expected some data, got nothing")
	} else {
		p := strings.Split(got, "\n")
		re := regexp.MustCompile(`^(\w+) = '(.+)'$`)
		for _, v := range p {
			if v == "" {
				continue
			}
			if !re.MatchString(v) {
				if !strings.HasPrefix(v, "DateStyle") && !strings.HasPrefix(v, "search_path") {
					t.Errorf("got misformed parameter: %s", v)
				}
			}
		}
	}
}

func TestDumpCreateDBAndACL(t *testing.T) {
	checkIfWeTest(t)

	var tests = []struct {
		db   string
		want string
	}{
		{"b1", "--\n-- Database creation\n--\n\nCREATE DATABASE \"b1\" WITH TEMPLATE = template0 OWNER = \"u1\" ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';\n\n"},
		{"b2", "--\n-- Database creation\n--\n\nCREATE DATABASE \"b2\" WITH TEMPLATE = template0 OWNER = \"u1\" ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';\n\n--\n-- Database privileges \n--\n\nREVOKE CONNECT, TEMPORARY ON DATABASE \"b2\" FROM PUBLIC;\nGRANT CONNECT ON DATABASE \"b2\" TO \"u2\";\n"},
	}

	for _, st := range tests {
		t.Run(fmt.Sprintf("%s", st.db), func(t *testing.T) {
			got, err := dumpCreateDBAndACL(testdb, st.db)
			if err != nil {
				t.Errorf("expected non nil error, got %q", err)
			}

			if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("dumpCreateDBAndACL() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Testing replication management fonctions needs a more complex setup
// so we skip it.
