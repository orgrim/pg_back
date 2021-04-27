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
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"testing"
)

func TestParseKeywordConnInfo(t *testing.T) {
	var tests = []struct {
		input string
		fail  string
		want  map[string]string
	}{
		{
			"host=/tmp port=5432",
			"",
			map[string]string{"host": "/tmp", "port": "5432"},
		},
		{
			"host='/tmp'",
			"",
			map[string]string{"host": "/tmp"},
		},
		{
			"host =  pg.local port\n = 5433 user=u1 dbname= 'silly name'  ",
			"",
			map[string]string{"dbname": "silly name", "host": "pg.local", "port": "5433", "user": "u1"},
		},
		{
			"bad keyword = value",
			"missing \"=\" after \"bad\"",
			map[string]string{},
		},
		{
			"bad-keyword=value",
			"illegal character in keyword",
			map[string]string{},
		},
		{
			"%bad_keyword=value",
			"illegal keyword character",
			map[string]string{},
		},
		{
			`key='\' \\ \p' other_key=ab\ cd`,
			"",
			map[string]string{"key": `' \ p`, "other_key": "ab cd"},
		},
		{
			"novalue ",
			"missing value",
			map[string]string{},
		},
		{
			"novalue",
			"missing value",
			map[string]string{},
		},
		{
			"key = 'no end quote",
			"unterminated quoted string",
			map[string]string{},
		},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := parseKeywordConnInfo(st.input)
			if err != nil {
				if len(st.fail) == 0 {
					t.Errorf("unexpected error: %v", err)
				} else {
					if err.Error() != st.fail {
						t.Errorf("unexpected error, got: %v, want: %v", err.Error(), st.fail)
					}
				}
			}

			if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("parseKeywordConnInfo() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseUrlConnInfo(t *testing.T) {
	var tests = []struct {
		input string
		want  map[string]string
	}{
		{
			"postgresql://",
			map[string]string{},
		},
		{
			"postgresql://localhost",
			map[string]string{"host": "localhost"},
		},
		{
			"postgresql://localhost:5433",
			map[string]string{"host": "localhost", "port": "5433"},
		},
		{
			"postgresql://localhost/mydb",
			map[string]string{"host": "localhost", "dbname": "mydb"},
		},
		{
			"postgresql://user@localhost",
			map[string]string{"host": "localhost", "user": "user"},
		},
		{
			"postgresql://user:secret@localhost",
			map[string]string{"host": "localhost", "user": "user", "password": "secret"},
		},
		{
			"postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp",
			map[string]string{"host": "localhost", "user": "other", "dbname": "otherdb", "connect_timeout": "10", "application_name": "myapp"},
		},
		{
			"postgresql://host1:123,host2:456/somedb?target_session_attrs=any&application_name=myapp",
			map[string]string{"host": "host1,host2", "port": "123,456", "dbname": "somedb", "target_session_attrs": "any", "application_name": "myapp"},
		},
		{
			"postgresql://[::1]:5433,[::1]:",
			map[string]string{"host": "::1,::1", "port": "5433,"},
		},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := parseUrlConnInfo(st.input)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(st.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("parseUrlConnInfo() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMakeKeywordConnInfo(t *testing.T) {
	var tests = []struct {
		infos map[string]string
		want  string
	}{
		{
			map[string]string{"host": "/tmp", "port": "5432", "dbname": "ab c'd", "password": "jE'r\\m"},
			`dbname='ab c\'d' host=/tmp password='jE\'r\\m' port=5432`,
		},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := makeKeywordConnInfo(st.infos)
			if got != st.want {
				t.Errorf("got: %v, want: %v", got, st.want)
			}
		})
	}
}

func TestMakeUrlConnInfo(t *testing.T) {
	var tests = []struct {
		infos map[string]string
		want  string
	}{
		{
			map[string]string{"host": "localhost", "port": "5432", "dbname": "db", "password": "secret"},
			"postgresql://:secret@localhost:5432/db",
		},
		{
			map[string]string{"host": "::1", "port": "5432", "dbname": "db", "password": "secret"},
			"postgresql://:secret@[::1]:5432/db",
		},
		{
			map[string]string{"host": "/tmp", "port": "5432", "dbname": "db"},
			"postgresql:///db?host=%2Ftmp&port=5432",
		},
		{
			map[string]string{"host": "h1,h2", "port": "5432", "dbname": "db", "user": "u1"},
			"postgresql://u1@h1:5432,h2:5432/db",
		},
		{
			map[string]string{"host": "h1,h2,h3", "port": "5432,5433", "dbname": "db", "user": "u1"},
			"postgresql://u1@h1:5432,h2:5433,h3/db",
		},
		{
			map[string]string{"host": "h1,h2,h3", "port": "5432,5433,", "dbname": "db", "user": "u1"},
			"postgresql://u1@h1:5432,h2:5433,h3/db",
		},
		{
			map[string]string{"host": "localhost", "port": "", "user": "u1", "password": "p"},
			"postgresql://u1:p@localhost/",
		},
		{
			map[string]string{},
			"postgresql:///",
		},
		{
			map[string]string{"user": "other", "host": "localhost", "dbname": "otherdb", "connect_timeout": "10", "application_name": "myapp"},
			"postgresql://other@localhost/otherdb?application_name=myapp&connect_timeout=10",
		},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := makeUrlConnInfo(st.infos)
			if got != st.want {
				t.Errorf("got: %v, want: %v", got, st.want)
			}
		})
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
		{"/tmp", 0, "", "", "application_name=pg_back host=/tmp"},
		{"localhost", 5432, "postgres", "postgres", "application_name=pg_back dbname=postgres host=localhost port=5432 user=postgres"},
		{"localhost", 0, "postgres", "postgres", "application_name=pg_back dbname=postgres host=localhost user=postgres"},
		{"localhost", 5432, "", "postgres", "application_name=pg_back dbname=postgres host=localhost port=5432"},
		{"localhost", 5432, "postgres", "", "application_name=pg_back host=localhost port=5432 user=postgres"},
		{"localhost", 0, "postgres", "", "application_name=pg_back host=localhost user=postgres"},
		{"", 0, "postgres", "", "application_name=pg_back host=/var/run/postgresql user=postgres"},
		{"localhost", 0, "postgres", "host=/tmp port=5432", "application_name=pg_back host=/tmp port=5432"},
		{"", 0, "", "host=/tmp port=5433 application_name=other", "application_name=other host=/tmp port=5433"},
		{"", 0, "", "postgresql:///db?host=/tmp", "postgresql:///db?application_name=pg_back&host=%2Ftmp"},
	}

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			res, _ := prepareConnInfo(subt.host, subt.port, subt.username, subt.dbname)
			if res.String() != subt.want {
				t.Errorf("got '%s', want '%s'", res, subt.want)
			}
		})
	}
}

func TestConnInfoCopy(t *testing.T) {
	want := &ConnInfo{
		Kind:  CI_KEYVAL,
		Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "db"},
	}

	got := want.Copy()

	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("*ConnInfo.Copy() mismatch (-want +got):\n%s", diff)
	}

	if want == got {
		t.Errorf("*ConnInfo.Copy() output is the same")
	}
}

func TestConnInfoSet(t *testing.T) {
	var tests = []struct {
		input *ConnInfo
		key   string
		val   string
		want  *ConnInfo
	}{
		{
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "db"},
			},
			"dbname",
			"other",
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "other"},
			},
		},
		{
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "db"},
			},
			"user",
			"somebody",
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "db", "user": "somebody"},
			},
		},
	}

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := subt.input.Set(subt.key, subt.val)
			if diff := cmp.Diff(subt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("*ConnInfo.Set() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestConnInfoDel(t *testing.T) {
	var tests = []struct {
		input *ConnInfo
		key   string
		want  *ConnInfo
	}{
		{
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "db"},
			},
			"dbname",
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432"},
			},
		},
		{
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "db"},
			},
			"user",
			&ConnInfo{
				Kind:  CI_KEYVAL,
				Infos: map[string]string{"host": "localhost", "port": "5432", "dbname": "db"},
			},
		},
	}

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := subt.input.Del(subt.key)
			if diff := cmp.Diff(subt.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("*ConnInfo.Del() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
