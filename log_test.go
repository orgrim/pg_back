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
	"bytes"
	"fmt"
	"os"
	"regexp"
	"testing"
)

func TestLevelLogSetVerbose(t *testing.T) {
	var tests = []bool{true, false}
	l := NewLevelLog()
	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			l.SetVerbose(subt)
			if l.verbose != subt {
				t.Errorf("got %v, want %v", l.verbose, subt)
			}
		})
	}
}

func TestLevelLogVerbose(t *testing.T) {
	var tests = []struct {
		verbose bool
		message string
		re      string
		fOrln   bool
	}{
		{true, "test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}.\d{6} DEBUG: test$`, true},
		{true, "test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}.\d{6} DEBUG: test$`, false},
		{false, "test", `^$`, true},
		{false, "test", `^$`, false},
	}

	l := NewLevelLog()

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			l.logger.SetOutput(buf)
			l.SetVerbose(subt.verbose)
			if subt.fOrln {
				l.Verbosef("%s", subt.message)
			} else {
				l.Verboseln(subt.message)
			}
			line := buf.String()
			if len(line) > 0 {
				line = line[0 : len(line)-1]
			}
			matched, err := regexp.MatchString(subt.re, line)
			if err != nil {
				t.Fatal("pattern did not compile:", err)
			}
			if !matched {
				t.Errorf("log output should match %q is %q", subt.re, line)
			}
			l.logger.SetOutput(os.Stderr)
		})
	}
}

func TestLevelLogInfo(t *testing.T) {
	var tests = []struct {
		message string
		re      string
		fOrln   bool
	}{
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: test$`, true},
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO: test$`, false},
	}

	l := NewLevelLog()

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			l.logger.SetOutput(buf)
			if subt.fOrln {
				l.Infof("%s", subt.message)
			} else {
				l.Infoln(subt.message)
			}
			line := buf.String()
			line = line[0 : len(line)-1]

			matched, err := regexp.MatchString(subt.re, line)
			if err != nil {
				t.Fatal("pattern did not compile:", err)
			}
			if !matched {
				t.Errorf("log output should match %q is %q", subt.re, line)
			}
			l.logger.SetOutput(os.Stderr)
		})
	}
}

func TestLevelLogWarn(t *testing.T) {
	var tests = []struct {
		message string
		re      string
		fOrln   bool
	}{
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} WARN: test$`, true},
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} WARN: test$`, false},
	}

	l := NewLevelLog()

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			l.logger.SetOutput(buf)
			if subt.fOrln {
				l.Warnf("%s", subt.message)
			} else {
				l.Warnln(subt.message)
			}
			line := buf.String()
			line = line[0 : len(line)-1]

			matched, err := regexp.MatchString(subt.re, line)
			if err != nil {
				t.Fatal("pattern did not compile:", err)
			}
			if !matched {
				t.Errorf("log output should match %q is %q", subt.re, line)
			}
			l.logger.SetOutput(os.Stderr)
		})
	}
}

func TestLevelLogError(t *testing.T) {
	var tests = []struct {
		message string
		re      string
		fOrln   bool
	}{
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: test$`, true},
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR: test$`, false},
	}

	l := NewLevelLog()

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			l.logger.SetOutput(buf)
			if subt.fOrln {
				l.Errorf("%s", subt.message)
			} else {
				l.Errorln(subt.message)
			}
			line := buf.String()
			line = line[0 : len(line)-1]

			matched, err := regexp.MatchString(subt.re, line)
			if err != nil {
				t.Fatal("pattern did not compile:", err)
			}
			if !matched {
				t.Errorf("log output should match %q is %q", subt.re, line)
			}
			l.logger.SetOutput(os.Stderr)
		})
	}
}

func TestLevelLogFatal(t *testing.T) {
	var tests = []struct {
		message string
		re      string
		fOrln   bool
	}{
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} FATAL: test$`, true},
		{"test", `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} FATAL: test$`, false},
	}

	l := NewLevelLog()

	for i, subt := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			l.logger.SetOutput(buf)
			if subt.fOrln {
				l.Fatalf("%s", subt.message)
			} else {
				l.Fatalln(subt.message)
			}
			line := buf.String()
			line = line[0 : len(line)-1]

			matched, err := regexp.MatchString(subt.re, line)
			if err != nil {
				t.Fatal("pattern did not compile:", err)
			}
			if !matched {
				t.Errorf("log output should match %q is %q", subt.re, line)
			}
			l.logger.SetOutput(os.Stderr)
		})
	}
}
