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
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"os"
)

func ChecksumFile(path string, algo string) error {
	var h hash.Hash

	switch algo {
	case "none":
		return nil
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
		return fmt.Errorf("Unsupported hash algorithm: %s", algo)
	}

	// Open the file and use io.Copy to feed the data to the hash,
	// like in the example of the doc, then write the result to a
	// file that the standard shaXXXsum tools can understand
	f, err := os.Open(path)
	if err != nil {
		l.Errorln(err)
		return err
	}
	defer f.Close()

	if _, err := io.Copy(h, f); err != nil {
		l.Errorln(err)
	}

	o, oerr := os.Create(fmt.Sprintf("%s.%s", path, algo))
	if oerr != nil {
		l.Errorln(oerr)
		return oerr
	}
	defer o.Close()

	fmt.Fprintf(o, "%x  %s\n", h.Sum(nil), path)
	return nil
}
