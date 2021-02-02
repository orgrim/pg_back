// pg_goback
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
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestLockPath(t *testing.T) {
	// Work from a tempdir
	dir, err := ioutil.TempDir("", "test_lockpath")
	if err != nil {
		t.Fatal("could not create tempdir:", err)
	}
	defer os.RemoveAll(dir)

	// tempdir with perms for mkdirall failure
	if err := os.MkdirAll(filepath.Join(dir, "subfail"), 0444); err != nil {
		t.Fatal("could not create temp subdir:", err)
	}

	_, _, err = lockPath(filepath.Join(dir, "subfail", "subfail", "lockfail"))
	var e *os.PathError
	if !errors.As(err, &e) {
		t.Errorf("expected a *os.PathError, got %q\n", err)
	}

	// path is subdir of tempdir to make os.create fail
	_, _, err = lockPath(filepath.Join(dir, "subfail"))
	if !errors.As(err, &e) {
		t.Errorf("expected a *os.PathError, got %q\n", err)
	}

	// lock a path with success
	f, l, err := lockPath(filepath.Join(dir, "lock"))
	if err != nil {
		t.Errorf("expected <nil> got error %q\n", err)
	}
	defer f.Close()
	if !l {
		t.Errorf("expected a true for locked, got false")
	}

	// fail to lock it again
	f1, l1, err := lockPath(filepath.Join(dir, "lock"))
	if err != nil {
		t.Errorf("expected <nil> got error %q\n", err)
	}
	if l1 {
		t.Errorf("expected a false for failed locked, got true")
	}
	f1.Close()
}

func TestUnlockPath(t *testing.T) {
	f, err := ioutil.TempFile("", "test_unlockpath")
	if err != nil {
		t.Fatal("could not create tempfile")
	}
	defer os.Remove(f.Name())

	// unlock shall always work even if the file is not locked
	err = unlockPath(f)
	if err != nil {
		t.Errorf("got error %q on non locked file\n", err)
	}

	// error when the locked file as already been removed
	os.Remove(f.Name())
	err = unlockPath(f)
	if err == nil {
		t.Errorf("got <nil> instead of \"bad file descriptor\" error")
	}
}
