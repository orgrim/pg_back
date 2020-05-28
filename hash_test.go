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
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

func TestChecksumFile(t *testing.T) {
	var tests = []struct {
		algo string
		tool string
	}{
		{"sha1", "sha1sum"},
		{"sha224", "sha224sum"},
		{"sha256", "sha256sum"},
		{"sha384", "sha384sum"},
		{"sha512", "sha512sum"},
	}

	// create a temporary directory to store a test file to
	// checksum with the different algorithm relatively
	dir, err := ioutil.TempDir("", "test_checksum_file")
	if err != nil {
		t.Fatal("could not create tempdir:", err)
	}
	defer os.RemoveAll(dir)

	var cwd string
	cwd, err = os.Getwd()
	if err != nil {
		t.Fatal("could not get current dir:", err)
	}

	err = os.Chdir(dir)
	if err != nil {
		t.Fatal("could not change to tempdir:", err)
	}
	defer os.Chdir(cwd)

	// create a test file
	if f, err := os.Create("test"); err != nil {
		t.Fatal("could not create test file")
	} else {
		fmt.Fprintf(f, "abdc\n")
		f.Close()
	}

	// bad algo
	if err := checksumFile("", "none"); err != nil {
		t.Errorf("expected <nil>, got %q\n", err)
	}

	if err := checksumFile("", "other"); err == nil {
		t.Errorf("expected err, got <nil>\n")
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			if err := checksumFile("test", st.algo); err != nil {
				t.Errorf("checksumFile returned: %v", err)
			}

			c := exec.Command(st.tool, "-c", "test."+st.algo)
			out, err := c.CombinedOutput()
			if err != nil {
				t.Errorf("check command failed: %s\n", out)
			}
			if string(out) != "test: OK\n" {
				t.Errorf("expected OK, got %q\n", out)
			}
		})
	}

	// bad files
	var e *os.PathError
	l.logger.SetOutput(ioutil.Discard)
	if err := checksumFile("", "sha1"); !errors.As(err, &e) {
		t.Errorf("expected an *os.PathError, got %q\n", err)
	}

	os.Chmod("test.sha1", 0444)
	if err := checksumFile("test", "sha1"); !errors.As(err, &e) {
		t.Errorf("expected an *os.PathError, got %q\n", err)
	}
	os.Chmod("test.sha1", 0644)
}
