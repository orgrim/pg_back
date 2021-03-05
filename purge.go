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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func purgeDumps(directory string, dbname string, keep int, limit time.Time) error {
	dirpath := filepath.Dir(formatDumpPath(directory, "", "", dbname, time.Time{}))
	dir, err := os.Open(dirpath)
	if err != nil {
		return fmt.Errorf("could not purge %s: %s", dirpath, err)
	}
	defer dir.Close()
	dirContents := make([]os.FileInfo, 0)
	for {
		var f []os.FileInfo
		f, err = dir.Readdir(1)
		if errors.Is(err, io.EOF) {
			// reset to avoid returning is.EOF at the end
			err = nil
			break
		} else if err != nil {
			return fmt.Errorf("could not purge %s: %s", dirpath, err)
		}

		if strings.HasPrefix(f[0].Name(), dbname+"_") &&
			(!f[0].IsDir() || strings.HasSuffix(f[0].Name(), ".d")) {
			dirContents = append(dirContents, f[0])
		}
	}

	// Sort the list of filenames by date, youngest first,
	// so that we can slice it easily to keep backups
	sort.Slice(dirContents, func(i, j int) bool {
		return dirContents[i].ModTime().After(dirContents[j].ModTime())
	})

	if keep < len(dirContents) && keep >= 0 {
		for _, f := range dirContents[keep:] {
			file := filepath.Join(dirpath, f.Name())
			if f.ModTime().Before(limit) {
				l.Infoln("removing", file)
				if f.IsDir() {
					if err = os.RemoveAll(file); err != nil {
						l.Errorln(err)
					}
				} else {
					if err = os.Remove(file); err != nil {
						l.Errorln(err)
					}
				}
			} else {
				l.Verboseln("keeping", file)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("could not purge %s: %s", dirpath, err)
	}
	return nil
}
