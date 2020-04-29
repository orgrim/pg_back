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
	"testing"
)

// TODO use postgres in a docker

func TestDbOpen(t *testing.T) {
	db, ok := DbOpen("host=/var/run/postgresql/")
	defer db.Close()
	if !ok {
		t.Errorf("expected an ok")
	}
}

func TestPrepareConnInfo(t *testing.T) {
	var tests = []struct {
		host string
		port int
		username string
		dbname string
		want string
	}{
		{"", 0, "", "", "host=/var/run/postgresql application_name=pg_goback"},
		{"localhost", 5432, "postgres", "postgres", "host=localhost port=5432 user=postgres dbname=postgres application_name=pg_goback"},
		{"localhost", 0, "postgres", "postgres", "host=localhost user=postgres dbname=postgres application_name=pg_goback"},
		{"localhost", 5432, "", "postgres", "host=localhost port=5432 dbname=postgres application_name=pg_goback"},
		{"localhost", 5432, "postgres", "", "host=localhost port=5432 user=postgres application_name=pg_goback"},
		{"localhost", 0, "postgres", "", "host=localhost user=postgres application_name=pg_goback"},
	}

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			res := PrepareConnInfo(subt.host, subt.port, subt.username, subt.dbname)
			if res != subt.want {
				t.Errorf("got '%s', want '%s'", res, subt.want)
			}
		})
	}
}
