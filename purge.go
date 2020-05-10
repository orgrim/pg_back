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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func PurgeValidateKeepValue(k string) int {
	// returning -1 means keep all dumps
	if k == "all" {
		return -1
	}

	if keep, err := strconv.ParseInt(k, 10, 0); err != nil {
		// return -1 too when the input is not convertible to an int, this way we avoid any
		l.Warnln("Invalid input for -K, keeping everything")
		return -1
	} else {
		return int(keep)
	}
}

func PurgeValidateTimeLimitValue(l string) (time.Duration, error) {

	if days, err := strconv.ParseInt(l, 10, 0); err != nil {
		if errors.Is(err, strconv.ErrRange) {
			// invalid
			return 0, errors.New("Invalid input for -P, number too big")
		}
	} else {
		return time.Duration(-days*24) * time.Hour, nil
	}

	if d, err := time.ParseDuration(l); err != nil {
		return 0, err
	} else {
		return -d, nil
	}
}

func PurgeDumps(directory string, dbname string, keep int, limit time.Time) error {
	dirpath := filepath.Dir(FormatDumpPath(directory, "", dbname, time.Time{}))
	dir, err := os.Open(dirpath)
	if err != nil {
		l.Errorln(err)
		return err
	}
	defer dir.Close()
	dirContents := make([]os.FileInfo, 0)
	for {
		var f []os.FileInfo
		f, err = dir.Readdir(1)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			l.Errorln(err)
			return err
		}

		if strings.HasPrefix(f[0].Name(), dbname+"_") && !f[0].IsDir() {
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
			if f.ModTime().Before(limit) {
				file := filepath.Join(dirpath, f.Name())
				if err = os.Remove(file); err != nil {
					l.Errorf("Could not remove %s: %v\n", file, err)
				}
			}
		}
	}
	return err
}
