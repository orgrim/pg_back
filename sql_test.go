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
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

var (
	pgdata string
	pgport int
	pgup   bool
	testdb *pg
)

// TODO use postgres in a docker or set it up with those two functions
func setupPostgres(t *testing.T) {
	if os.Getenv("PGBK_TEST_PG") != "1" {
		t.Skip("testing with PostgreSQL disabled")
	}

	dir, err := ioutil.TempDir("", "test_sql")
	if err != nil {
		t.Fatal("could not prepare tempdir:", err)
	}

	pgdata = filepath.Join(dir, "pgdata")
	pgport = rand.Intn(10000) + 30000
	pglog := filepath.Join(dir, "log")

	initdb := exec.Command("initdb", pgdata)
	err = initdb.Run()
	var rc *exec.ExitError
	if err != nil {
		if errors.As(err, &rc) {
			t.Fatal("initdb exited with code", rc.ExitCode(), "stderr\n", rc.Stderr)
		}
		t.Fatal("initdb failed:", err)
	}

	pgctl := exec.Command("pg_ctl", "-D", pgdata, "-o", fmt.Sprintf("-p %d", pgport), "-l", pglog, "start")
	err = pgctl.Run()
	if err != nil {
		if errors.As(err, &rc) {
			t.Fatal("pg_ctl exited with code", rc.ExitCode(), "stderr\n", rc.Stderr)
		}
		t.Fatal("pg_ctl failed:", err)
	}
	pgup = true
	t.Cleanup(teardownPostgres)
	fmt.Println("setup postgres done")
}

func teardownPostgres() {
	pgctl := exec.Command("pg_ctl", "-D", pgdata, "stop", "-m", "immediate")
	var rc *exec.ExitError
	err := pgctl.Run()
	if err != nil {
		if errors.As(err, &rc) {
			log.Fatalln("pg_ctl exited with code", rc.ExitCode(), "stderr\n", rc.Stderr)
		}
		log.Fatalln("pg_ctl failed:", err)
	}
	os.RemoveAll(filepath.Dir(pgdata))
	fmt.Println("teardown postgres done")
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

	var tests = []struct {
		templates bool
		want      []string
	}{
		{false, []string{"b1", "postgres"}},
		{true, []string{"b1", "postgres", "template1"}},
	}

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

/*
dumpCreateDBAndACL
dumpDBConfig
showSettings
pauseReplication
canPauseReplication
pauseReplicationWithTimeout
resumeReplication
*/
