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
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
)

func computeChecksum(path string, h hash.Hash) (string, error) {
	h.Reset()

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return string(h.Sum(nil)), nil
}

func checksumFile(path string, mode int, algo string) (string, error) {
	var h hash.Hash

	switch algo {
	case "none":
		return "", nil
	case "sha1":
		h = sha1.New()
	case "sha224":
		h = sha256.New224()
	case "sha256":
		h = sha256.New()
	case "sha384":
		h = sha512.New384()
	case "sha512":
		h = sha512.New()
	default:
		return "", fmt.Errorf("unsupported hash algorithm: %s", algo)
	}

	i, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	sumFile := fmt.Sprintf("%s.%s", path, algo)
	l.Verbosef("create checksum file: %s", sumFile)
	o, err := os.Create(sumFile)
	if err != nil {
		l.Errorln(err)
		return "", err
	}
	defer o.Close()

	if i.IsDir() {
		l.Verboseln("dump is a directory, checksumming all file inside")
		err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				l.Verboseln("computing checksum of:", path)
				r, cerr := computeChecksum(path, h)
				if cerr != nil {
					return fmt.Errorf("could not checksum %s: %s", path, cerr)
				}
				fmt.Fprintf(o, "%x *%s\n", r, path)
			}
			return nil
		})

		if err != nil {
			return "", fmt.Errorf("error walking the path %q: %v\n", path, err)
		}
	} else {

		// Open the file and use io.Copy to feed the data to the hash,
		// like in the example of the doc, then write the result to a
		// file that the standard shaXXXsum tools can understand
		l.Verboseln("computing checksum of:", path)
		r, _ := computeChecksum(path, h)
		fmt.Fprintf(o, "%x  %s\n", r, path)
	}
	l.Verboseln("computing checksum with MODE", mode, path)
	if mode > 0 {
		if err := os.Chmod(o.Name(), os.FileMode(mode)); err != nil {
			return "", fmt.Errorf("could not chmod checksum file %s: %s", path, err)
		}
	}
	return sumFile, nil
}

func checksumFileList(paths []string, mode int, algo string, sumFilePrefix string) (string, error) {
	var h hash.Hash

	switch algo {
	case "none":
		return "", nil
	case "sha1":
		h = sha1.New()
	case "sha224":
		h = sha256.New224()
	case "sha256":
		h = sha256.New()
	case "sha384":
		h = sha512.New384()
	case "sha512":
		h = sha512.New()
	default:
		return "", fmt.Errorf("unsupported hash algorithm: %s", algo)
	}

	sumPath := fmt.Sprintf("%s.%s", sumFilePrefix, algo)
	l.Verbosef("create or use checksum file: %s", sumPath)
	o, err := os.OpenFile(sumPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return "", fmt.Errorf("could not open %s: %w", sumPath, err)
	}

	defer o.Close()

	failed := false
	for _, path := range paths {
		l.Verboseln("computing checksum of:", path)
		r, err := computeChecksum(path, h)
		if err != nil {
			l.Errorf("could not checksum %s: %s", path, err)
			failed = true
			continue
		}

		fmt.Fprintf(o, "%x *%s\n", r, path)

		if mode > 0 {
			if err := os.Chmod(o.Name(), os.FileMode(mode)); err != nil {
				return "", fmt.Errorf("could not chmod checksum file %s: %s", path, err)
			}
		}
	}

	if failed {
		return "", fmt.Errorf("computing of checksum failed. Please examine output")
	}

	return sumPath, nil
}
