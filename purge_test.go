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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// func purgeDumps(directory string, dbname string, keep int, limit time.Time) error
func TestPurgeDumps(t *testing.T) {
	// work in a tempdir
	dir, err := ioutil.TempDir("", "test_purge_dumps")
	if err != nil {
		t.Fatal("could not create tempdir:", err)
	}
	defer os.RemoveAll(dir)

	// empty path
	wd := filepath.Join(dir, "real", "bad")
	if err := os.MkdirAll(wd, 0755); err != nil {
		t.Fatal("could not create test dir")
	}
	os.Chmod(filepath.Dir(wd), 0444)
	err = purgeDumps(wd, "", 0, time.Time{})
	if err == nil {
		t.Errorf("empty path gave error <nil>\n")
	}
	os.Chmod(filepath.Dir(wd), 0755)

	// empty dbname
	tf := formatDumpPath(wd, time.RFC3339, "dump", "", time.Now().Add(-time.Hour))
	err = purgeDumps(wd, "", 0, time.Now())
	if err != nil {
		t.Errorf("empty dbname gave error %s", err)
	}
	if _, err := os.Stat(tf); err == nil {
		t.Errorf("file still exists")
	}

	// file without write perms
	tf = formatDumpPath(wd, time.RFC3339, "dump", "db", time.Now().Add(-time.Hour))
	ioutil.WriteFile(tf, []byte("truc\n"), 0644)
	os.Chmod(filepath.Dir(tf), 0555)

	err = purgeDumps(wd, "db", 0, time.Now())
	if err == nil {
		t.Errorf("bad perms on file did not gave an error")
	}
	os.Chmod(filepath.Dir(tf), 0755)

	// dir without write perms
	tf = formatDumpPath(wd, time.RFC3339, "d", "db", time.Now().Add(-time.Hour))
	os.MkdirAll(tf, 0755)
	os.Chmod(filepath.Dir(tf), 0555)

	err = purgeDumps(wd, "db", 0, time.Now())
	if err == nil {
		t.Errorf("bad perms on dir did not gave an error")
	}
	os.Chmod(filepath.Dir(tf), 0755)

	// time and keep limits
	var tests = []struct {
		keep  int
		limit time.Time
		want  int
	}{
		{0, time.Time{}, 3},
		{1, time.Time{}, 3},
		{0, time.Now().Add(-time.Minute * time.Duration(90)), 1},
		{1, time.Now().Add(-time.Minute * time.Duration(90)), 1},
		{2, time.Now().Add(-time.Minute * time.Duration(90)), 2},
		{3, time.Now().Add(-time.Minute * time.Duration(90)), 3},
		{-1, time.Now(), 3},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			// create 3 files, 1 per hour
			wd = filepath.Join(dir, "wd")
			if err := os.MkdirAll(wd, 0755); err != nil {
				t.Fatal("could not create test dir")
			}
			for i := 1; i <= 3; i++ {
				when := time.Now().Add(-time.Hour * time.Duration(i))
				tf = formatDumpPath(wd, time.RFC3339, "dump", "db", when)
				ioutil.WriteFile(tf, []byte("truc\n"), 0644)
				os.Chtimes(tf, when, when)
			}

			if err := purgeDumps(wd, "db", st.keep, st.limit); err != nil {
				t.Errorf("purgeDumps returned: %v", err)
			}

			dir, err := os.Open(wd)
			if err != nil {
				t.Fatal("could not open workdir:", err)
			}
			defer dir.Close()

			fi, err := dir.Readdir(-1)
			if err != nil {
				t.Fatal("could not read workdir:", err)
			}
			if len(fi) != st.want {
				var info string
				for _, f := range fi {
					info += fmt.Sprintf("%s\n", f.Name())
				}
				t.Errorf("expected %d files in dir, found %d\n%slimit: %v", st.want, len(fi), info, st.limit)
			}

			os.RemoveAll(wd)
		})
	}
}
