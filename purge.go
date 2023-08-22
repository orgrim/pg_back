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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type purgeJob struct {
	datetime time.Time
	dirs     []string
	files    []string
}

func genPurgeJobs(items []Item, dbname string) []purgeJob {
	jobs := make(map[string]purgeJob)

	// The files to purge must be grouped by date. depending on the options
	// there can be up to 6 files for a database or output
	reExt := regexp.MustCompile(`^(sql|d|dump|tar|out|createdb\.sql)(?:\.(sha\d{1,3}|age))?(?:\.(sha\d{1,3}|age))?(?:\.(sha\d{1,3}))?`)

	for _, item := range items {
		if strings.HasPrefix(item.key, cleanDBName(dbname)+"_") {
			dateNExt := strings.TrimPrefix(item.key, cleanDBName(dbname)+"_")
			parts := strings.SplitN(dateNExt, ".", 2)

			var (
				date   time.Time
				parsed bool
			)

			// We match the file using every timestamp format
			// possible so that the format can be changed without
			// breaking the purge
			for _, layout := range []string{"2006-01-02_15-04-05", time.RFC3339} {

				// Parse the format to a time in the local
				// timezone when the timezone is not part of
				// the string, otherwise it uses to timezone
				// written in the string. We do this because
				// the limit is in the local timezone.
				date, _ = time.ParseInLocation(layout, parts[0], time.Local)
				if !date.IsZero() {
					parsed = true
					break
				}
			}

			if !parsed {
				// the file does not match the time format, skip it
				continue
			}

			// Identify the kind of file based on the dot separated
			// strings at the end of its name
			matches := reExt.FindStringSubmatch(parts[1])
			if len(matches) == 5 {
				job := jobs[parts[0]]

				if job.datetime.IsZero() {
					job.datetime = date
				}

				if date.Before(job.datetime) {
					job.datetime = date
				}

				if item.isDir {
					job.dirs = append(job.dirs, item.key)
				} else {
					job.files = append(job.files, item.key)
				}

				jobs[parts[0]] = job
				continue
			}
		}
	}

	// The output is a list of jobs, sorted by date, youngest first
	jobList := make([]purgeJob, 0)
	for _, j := range jobs {
		jobList = append(jobList, j)
	}

	sort.Slice(jobList, func(i, j int) bool {
		return jobList[i].datetime.After(jobList[j].datetime)
	})

	return jobList
}

func purgeDumps(directory string, dbname string, keep int, limit time.Time) error {
	l.Verboseln("purge:", dbname, "limit:", limit, "keep:", keep)

	// The dbname can be put in the path of the backup directory, so we
	// have to compute it first. This is why a dbname is required to purge
	// old dumps
	dirpath := filepath.Dir(formatDumpPath(directory, "", "", dbname, time.Time{}, 0))
	dir, err := os.Open(dirpath)
	if err != nil {
		return fmt.Errorf("could not purge %s: %s", dirpath, err)
	}
	defer dir.Close()

	files := make([]Item, 0)
	for {
		var f []os.FileInfo
		f, err = dir.Readdir(1)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// reset to avoid returning is.EOF at the end
				err = nil
				break
			}
			return fmt.Errorf("could not purge %s: %s", dirpath, err)
		}

		files = append(files, Item{key: f[0].Name(), modtime: f[0].ModTime(), isDir: f[0].IsDir()})
	}

	// Parse and group by date. We remove groups of files produced by
	// the same run (including checksums, encrypted files, etc)
	jobs := genPurgeJobs(files, dbname)

	if keep < len(jobs) && keep >= 0 {
		// Show the files kept in verbose mode
		for _, j := range jobs[:keep] {
			for _, f := range j.files {
				l.Verboseln("keeping (count)", filepath.Join(dirpath, f))
			}

			for _, d := range j.dirs {
				l.Verboseln("keeping (count)", filepath.Join(dirpath, d))
			}
		}

		// Purge the older files that after excluding the one we need
		// to keep
		for _, j := range jobs[keep:] {
			if j.datetime.Before(limit) {
				for _, f := range j.files {
					path := filepath.Join(dirpath, f)
					l.Infoln("removing", path)
					if err = os.Remove(path); err != nil {
						l.Errorln(err)
					}
				}

				for _, d := range j.dirs {
					path := filepath.Join(dirpath, d)
					l.Infoln("removing", path)
					if err = os.RemoveAll(path); err != nil {
						l.Errorln(err)
					}
				}
			} else {
				for _, f := range j.files {
					l.Verboseln("keeping (age)", filepath.Join(dirpath, f))
				}

				for _, d := range j.dirs {
					l.Verboseln("keeping (age)", filepath.Join(dirpath, d))
				}
			}
		}
	}

	if err != nil {
		return fmt.Errorf("could not purge %s: %s", dirpath, err)
	}

	return nil
}

func purgeRemoteDumps(repo Repo, directory string, dbname string, keep int, limit time.Time) error {
	l.Verboseln("remote purge:", dbname, "limit:", limit, "keep:", keep)

	// The dbname can be put in the directory tree of the dump, in this
	// case the directory containing {dbname} in its name is kept on the
	// remote path along with any subdirectory. So we have to include it in
	// the filter when listing remote files
	dirpath := filepath.Dir(formatDumpPath(directory, "", "", dbname, time.Time{}, 0))
	prefix := relPath(directory, filepath.Join(dirpath, cleanDBName(dbname)))

	// Get the list of files from the repository, this includes the
	// contents of dumps in the directory format.
	remoteFiles, err := repo.List(prefix)
	if err != nil {
		return fmt.Errorf("could not purge: %w", err)
	}

	// We are going to parse the filename, we need to remove any posible
	// parent dir before the name of the dump
	parentDir := filepath.Dir(prefix)
	if parentDir == "." || parentDir == "/" {
		parentDir = ""
	}

	files := make([]Item, 0)
	for _, i := range remoteFiles {
		f, err := filepath.Rel(parentDir, i.key)
		if err != nil {
			l.Warnf("could not process remote file %s: %s", i.key, err)
			continue
		}

		files = append(files, Item{key: f, modtime: i.modtime, isDir: i.isDir})
	}

	// Parse and group by date. We remove groups of files produced by
	// the same run (including checksums, encrypted files, etc)
	jobs := genPurgeJobs(files, dbname)

	if keep < len(jobs) && keep >= 0 {
		// Show the files kept in verbose mode
		for _, j := range jobs[:keep] {
			for _, f := range j.files {
				l.Verboseln("keeping remote (count)", filepath.Join(parentDir, f))
			}

			for _, d := range j.dirs {
				l.Verboseln("keeping remote (count)", filepath.Join(parentDir, d))
			}
		}

		// Purge the older files that after excluding the one we need
		// to keep
		for _, j := range jobs[keep:] {
			if j.datetime.Before(limit) {
				for _, f := range j.files {
					path := filepath.Join(parentDir, f)
					l.Infoln("removing remote", path)
					if err = repo.Remove(path); err != nil {
						l.Errorln(err)
					}
				}

				for _, d := range j.dirs {
					path := filepath.Join(parentDir, d)
					l.Infoln("removing remote", path)
					if err = repo.Remove(path); err != nil {
						l.Errorln(err)
					}
				}

			} else {
				for _, f := range j.files {
					l.Verboseln("keeping remote (age)", filepath.Join(parentDir, f))
				}

				for _, d := range j.dirs {
					l.Verboseln("keeping remote (age)", filepath.Join(parentDir, d))
				}
			}
		}
	}

	if err != nil {
		return fmt.Errorf("could not purge: %w", err)
	}

	return nil
}
