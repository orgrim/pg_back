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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"testing"
)

func TestHookCommand(t *testing.T) {
	var tests = []struct {
		cmd string
		re  string
	}{
		{"echo 'a'", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: test: a\n$`},
		{"echo a'", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: No closing quotation\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: No closing quotation\n$`},
		{"echo -e 'a\nb'", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: test: a\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: test: b\n$`},
		{"", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: unable to run an empty command\n$`},
		{"/nothingBLA a", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: fork/exec /nothingBLA: no such file or directory\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: fork/exec /nothingBLA: no such file or directory\n$`},
		{"sh -c 'echo test; exit 1'", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: test\n\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: exit status 1\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: exit status 1\n$`},
	}

	//	l := NewLevelLog()
	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			l.logger.SetOutput(buf)

			if err := HookCommand(subt.cmd, "test:"); err != nil {
				l.Errorln(err)
			}

			lines := buf.String()
			matched, err := regexp.MatchString(subt.re, lines)
			if err != nil {
				t.Fatal("pattern did not compile:", err)
			}
			if !matched {
				t.Errorf("expected a match of %q, got %q\n", subt.re, lines)
			}
			l.logger.SetOutput(os.Stderr)
		})
	}
}

func TestPreBackupHook(t *testing.T) {
	var tests = []struct {
		cmd   string
		re    string
		fails bool
	}{
		{"echo 'a'", `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: running pre-backup command: echo 'a'\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: pre-backup: a\n$`, false},
		{"", "", false},
		{"/nothingBLA a", `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: running pre-backup command: /nothingBLA a\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: fork/exec /nothingBLA: no such file or directory\n\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} FATAL: hook command failed, exiting\n$`, true},
	}
	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			l.logger.SetOutput(buf)

			if err := PreBackupHook(subt.cmd); err != nil {
				if !subt.fails {
					t.Errorf("function test must not fail, got error: %q\n", err)
				}
			} else {
				if subt.fails {
					t.Errorf("function test must fail, it did not\n")
				}
			}

			lines := buf.String()
			matched, err := regexp.MatchString(subt.re, lines)
			if err != nil {
				t.Fatal("pattern did not compile:", err)
			}
			if !matched {
				t.Errorf("expected a match of %q, got %q\n", subt.re, lines)
			}
			l.logger.SetOutput(os.Stderr)
		})
	}
}

func TestPostBackupHook(t *testing.T) {
	t.Run("0", func(t *testing.T) {
		if os.Getenv("TEST_HOOK") == "1" {
			PostBackupHook("false")
			return
		}
		cmd := exec.Command(os.Args[0], "-test.run=TestPostBackupHook")
		cmd.Env = append(os.Environ(), "TEST_HOOK=1")
		err := cmd.Run()
		if e, ok := err.(*exec.ExitError); ok && !e.Success() {
			return
		}
		t.Fatalf("process ran with err %v, want exit status 1", err)
	})

	t.Run("1", func(t *testing.T) {
		buf := new(bytes.Buffer)
		l.logger.SetOutput(buf)
		PostBackupHook("")
		lines := buf.String()
		if len(lines) != 0 {
			t.Errorf("did not expect any output, got %q\n", lines)
		}
	})
}
