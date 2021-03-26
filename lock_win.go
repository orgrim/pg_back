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

// +build windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// lockPath on windows just creates a file without locking, it only tests if
// the file exist to consider it locked
func lockPath(path string) (*os.File, bool, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, false, err
	}

	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return nil, false, &os.PathError{Op: "stat", Path: path, Err: fmt.Errorf("unexpected directory")}
		}
		return nil, false, err
	}

	l.Verboseln("creating lock file", path)
	f, err := os.Create(path)
	if err != nil {
		return nil, false, err
	}
	return f, true, nil
}

// unlockPath releases the lock from the open file and removes the
// underlying path
func unlockPath(f *os.File) error {
	path := f.Name()
	l.Verboseln("removing lock file", path)
	f.Close()
	return os.Remove(path)
}
