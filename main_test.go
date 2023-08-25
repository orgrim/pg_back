// pg_back
//
// Copyright 2011-2022 Nicolas Thauvin and contributors. All rights reserved.
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
	"runtime"
	"testing"
)

func TestExecPath(t *testing.T) {
	var tests []struct {
		dir  string
		prog string
		want string
	}

	if runtime.GOOS != "windows" {
		tests = []struct {
			dir  string
			prog string
			want string
		}{
			{"", "pg_dump", "pg_dump"},
			{"/path/to/bin", "prog", "/path/to/bin/prog"},
		}
	} else {
		tests = []struct {
			dir  string
			prog string
			want string
		}{
			{"", "pg_dump", "pg_dump.exe"},
			{"C:\\path\\to\\bin", "prog", "C:\\path\\to\\bin\\prog.exe"},
		}
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			binDir = st.dir
			got := execPath(st.prog)
			if got != st.want {
				t.Errorf("expected %q, got %q\n", st.want, got)
			}
		})
	}
}

func TestEnsureCipherParamsPresent_NoEncryptNoDecrypt_NoParams_ReturnsNil(t *testing.T) {
	opts := options{}

	err := ensureCipherParamsPresent(&opts)
	if err != nil {
		t.Errorf("should not return error")
	}
}

func TestEnsureCipherParamsPresent_NoEncryptNoDecrypt_HasParams_ReturnsNil(t *testing.T) {
	opts := options{
		CipherPublicKey:  "foo1",
		CipherPrivateKey: "bar99",
		CipherPassphrase: "secretwords",
	}

	err := ensureCipherParamsPresent(&opts)
	if err != nil {
		t.Errorf("should not return error")
	}
}

func TestEnsureCipherParamsPresent_Encrypt_NoParams_Failure(t *testing.T) {
	opts := options{
		Encrypt:          true,
		CipherPrivateKey: "bar99",
	}

	err := ensureCipherParamsPresent(&opts)
	if err == nil {
		t.Errorf("should have error about not finding passphrase")
	}
}

func TestEnsureCipherParamsPresent_Encrypt_NoParamsButEnv_Success(t *testing.T) {
	opts := options{
		Encrypt: true,
	}
	t.Setenv("PGBK_CIPHER_PASS", "works")

	err := ensureCipherParamsPresent(&opts)
	if err != nil {
		t.Errorf("should have read environment variable")
	}

	if opts.CipherPassphrase != "works" {
		t.Errorf("passphrase was not read correctly from environment")
	}
}
