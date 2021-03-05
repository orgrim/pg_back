// pg_back
//
// Copyright 2020-2021 Nicolas Thauvin. All rights reserved.
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

func TestParseConnInfo(t *testing.T) {
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
			got, err := parseConnInfo(st.input)
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
				t.Errorf("parseConnInfo() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMakeConnInfo(t *testing.T) {
	var tests = []struct {
		infos map[string]string
		want  string
	}{
		{
			map[string]string{"host": "/tmp", "port": "5432", "dbname": "ab c'd", "password": "jE'r\\m"},
			`dbname='ab c\'d' host=/tmp password=jE\'r\\m port=5432`,
		},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := makeConnInfo(st.infos)
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
		{"/tmp", 0, "", "", "host=/tmp application_name=pg_back"},
		{"localhost", 5432, "postgres", "postgres", "host=localhost port=5432 user=postgres dbname=postgres application_name=pg_back"},
		{"localhost", 0, "postgres", "postgres", "host=localhost user=postgres dbname=postgres application_name=pg_back"},
		{"localhost", 5432, "", "postgres", "host=localhost port=5432 dbname=postgres application_name=pg_back"},
		{"localhost", 5432, "postgres", "", "host=localhost port=5432 user=postgres application_name=pg_back"},
		{"localhost", 0, "postgres", "", "host=localhost user=postgres application_name=pg_back"},
		{"", 0, "postgres", "", "host=/tmp user=postgres application_name=pg_back"},
		{"localhost", 0, "postgres", "host=/tmp port=5432", "host=/tmp port=5432 application_name=pg_back"},
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
