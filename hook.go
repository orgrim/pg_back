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
	"github.com/anmitsu/go-shlex"
	"os"
	"os/exec"
	"strings"
)

func HookCommand(cmd string, logPrefix string) error {

	words, lerr := shlex.Split(cmd, true)
	if lerr != nil {
		l.Errorln(lerr)
		return lerr
	}

	prog := words[0]
	args := words[1:]

	c := exec.Command(prog, args...)
	stdoutStderr, err := c.CombinedOutput()
	if err != nil {
		if len(stdoutStderr) != 0 {
			l.Errorf("%s\n", stdoutStderr)
		}
		l.Errorln(err)
		return err
	} else {
		if len(stdoutStderr) > 0 {
			out := strings.Trim(string(stdoutStderr), "\n")
			for _, line := range strings.Split(out, "\n") {
				l.Infoln(logPrefix, line)
			}
		}
	}
	return nil
}

func PreBackupHook(cmd string) error {
	if cmd != "" {
		l.Infoln("running pre-backup command:", cmd)
		if err := HookCommand(cmd, "pre-backup:"); err != nil {
			l.Fatalln("hook command failed, exiting")
			return err
		}
	}
	return nil
}

func PostBackupHook(cmd string) {
	if cmd != "" {
		l.Infoln("running post-backup command:", cmd)
		if err := HookCommand(cmd, "post-backup:"); err != nil {
			l.Fatalln("hook command failed, exiting")
			os.Exit(1)
		}
	}
}
